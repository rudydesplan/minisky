package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	maxWorkflowSteps    = 1000
	maxHTTPResponseSize = 1 << 20
)

// noRedirectClient is used for workflow HTTP calls with SSRF protection.
var noRedirectClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, "127.0.0.1:8080")
	}},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return fmt.Errorf("redirects are not followed (SSRF protection)")
	},
}

// ExecuteWorkflow runs a workflow's source contents asynchronously and updates execution state.
func (api *API) ExecuteWorkflow(ctx context.Context, execName, sourceContents, argument string) {
	defer api.finishExecution(execName)
	if err := api.executeWorkflow(ctx, execName, sourceContents, argument); err != nil {
		log.Printf("[Workflows] execution %s failed: %v", execName, err)
	}
}

func (api *API) executeWorkflow(ctx context.Context, execName, sourceContents, argument string) error {
	result, err := runWorkflow(ctx, sourceContents, argument)

	api.mu.Lock()
	exec := api.executions[execName]
	updated := false
	if exec != nil && exec.State == "ACTIVE" {
		updated = true
		exec.EndTime = time.Now().UTC().Format(time.RFC3339Nano)
		if err != nil {
			if ctx.Err() != nil {
				exec.State = "CANCELLED"
			} else {
				exec.State = "FAILED"
				exec.Error = &ExecutionError{
					Payload: err.Error(),
					Context: execName,
				}
			}
		} else {
			exec.State = "SUCCEEDED"
			exec.Result = result
		}
	}
	delete(api.eventAdmissions, execName)
	api.mu.Unlock()
	if !updated && err == nil {
		err = fmt.Errorf("execution %q did not remain active through completion", execName)
	}

	if persistErr := api.persistState(); persistErr != nil {
		return fmt.Errorf("persist execution outcome: %w", persistErr)
	}
	return err
}

// CreateExecutionFromEvent implements the Eventarc WorkflowsExecutor interface.
// The stable delivery ID makes admission idempotent across daemon restarts.
func (api *API) CreateExecutionFromEvent(workflowName, eventPayload, deliveryID string) error {
	if !validEventDeliveryID(deliveryID) {
		return fmt.Errorf("invalid Eventarc delivery ID")
	}
	api.eventExecutionMu.Lock()
	defer api.eventExecutionMu.Unlock()

	api.mu.RLock()
	wf := api.workflows[workflowName]
	if wf != nil {
		wf = deepCopyWorkflow(wf)
	}
	api.mu.RUnlock()
	if wf == nil {
		return fmt.Errorf("workflow not found: %s", workflowName)
	}

	execID := "event-" + deliveryID
	execName := fmt.Sprintf("%s/executions/%s", workflowName, execID)
	sourceContents := wf.SourceContents
	api.mu.RLock()
	existing := api.executions[execName]
	admission := api.eventAdmissions[execName]
	if existing != nil {
		existing = deepCopyExecution(existing)
	}
	if admission != nil {
		clone := *admission
		admission = &clone
	}
	api.mu.RUnlock()
	if existing != nil {
		if existing.Argument != eventPayload {
			return fmt.Errorf("Eventarc delivery ID %q conflicts with an existing execution", deliveryID)
		}
		switch existing.State {
		case "SUCCEEDED":
			return nil
		case "ACTIVE":
			if admission == nil || admission.DeliveryID != deliveryID ||
				admission.Phase != eventAdmissionAdmitted {
				return fmt.Errorf("Eventarc execution %q is active but not safely resumable", execName)
			}
			if existing.WorkflowRevisionID != wf.RevisionID {
				return fmt.Errorf("Eventarc execution %q workflow revision changed before resume", execName)
			}
		default:
			return fmt.Errorf("Eventarc execution %q is terminal with state %s", execName, existing.State)
		}
	}

	if existing == nil {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		exec := &Execution{
			Name:               execName,
			StartTime:          now,
			State:              "ACTIVE",
			Argument:           eventPayload,
			WorkflowRevisionID: wf.RevisionID,
		}

		api.mu.Lock()
		api.executions[execName] = exec
		api.eventAdmissions[execName] = &eventAdmission{
			DeliveryID: deliveryID,
			Phase:      eventAdmissionAdmitted,
		}
		api.mu.Unlock()

		if err := api.persistState(); err != nil {
			api.mu.Lock()
			delete(api.executions, execName)
			delete(api.eventAdmissions, execName)
			api.mu.Unlock()
			api.compensateState(err)
			return fmt.Errorf("persist event execution admission: %w", err)
		}
		if api.afterEventAdmission != nil {
			if err := api.afterEventAdmission(execName, deliveryID); err != nil {
				return err
			}
		}
	}

	api.mu.Lock()
	currentAdmission := api.eventAdmissions[execName]
	if currentAdmission == nil || currentAdmission.DeliveryID != deliveryID ||
		currentAdmission.Phase != eventAdmissionAdmitted {
		api.mu.Unlock()
		return fmt.Errorf("Eventarc execution %q admission is not safely resumable", execName)
	}
	currentAdmission.Phase = eventAdmissionRunning
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		api.compensateState(err)
		return fmt.Errorf("persist event execution start: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	api.startExecution(execName, cancel)
	defer api.finishExecution(execName)
	return api.executeWorkflow(ctx, execName, sourceContents, eventPayload)
}

const eventAdmissionPauseFileEnv = "MINISKY_TEST_WORKFLOWS_ADMISSION_PAUSE_FILE"

func configureEventAdmissionPause(api *API) {
	pauseFile := strings.TrimSpace(os.Getenv(eventAdmissionPauseFileEnv))
	if pauseFile == "" || os.Getenv("MINISKY_PHASE18_EVENT_DELIVERY_INTEGRATION") != "1" {
		return
	}
	api.afterEventAdmission = func(executionName, deliveryID string) error {
		marker, err := json.Marshal(map[string]string{
			"deliveryId":    deliveryID,
			"executionName": executionName,
		})
		if err != nil {
			return err
		}
		if err := os.WriteFile(pauseFile, marker, 0o600); err != nil {
			return fmt.Errorf("write Eventarc admission pause marker: %w", err)
		}
		releaseFile := pauseFile + ".release"
		for {
			if _, err := os.Stat(releaseFile); err == nil {
				return nil
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("inspect Eventarc admission release marker: %w", err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

func validEventDeliveryID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// runWorkflow interprets a JSON workflow definition.
func runWorkflow(ctx context.Context, sourceContents, argument string) (string, error) {
	if sourceContents == "" {
		return "{}", nil
	}

	var steps []map[string]any
	if err := json.Unmarshal([]byte(sourceContents), &steps); err != nil {
		return "", fmt.Errorf("workflow source is not valid JSON: %w", err)
	}

	return executeSteps(ctx, steps, argument)
}

// executeSteps processes a list of workflow steps.
func executeSteps(ctx context.Context, steps []map[string]any, argument string) (string, error) {
	if len(steps) > maxWorkflowSteps {
		return "", fmt.Errorf("workflow exceeds maximum of %d steps", maxWorkflowSteps)
	}
	vars := map[string]any{}
	if argument != "" {
		var argData any
		if err := json.Unmarshal([]byte(argument), &argData); err == nil {
			vars["args"] = argData
		} else {
			vars["args"] = argument
		}
	}

	var lastResult string
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		for stepType, stepValue := range step {
			switch stepType {
			case "assign":
				assignments, ok := stepValue.(map[string]any)
				if !ok {
					continue
				}
				for k, v := range assignments {
					vars[k] = resolveExpression(v, vars)
				}
			case "return":
				result := resolveExpression(stepValue, vars)
				raw, _ := json.Marshal(result)
				return string(raw), nil
			case "call":
				callDef, ok := stepValue.(map[string]any)
				if !ok {
					continue
				}
				result, err := executeCall(ctx, callDef, vars)
				if err != nil {
					return "", err
				}
				if resultName, ok := callDef["result"].(string); ok {
					vars[resultName] = result
				}
				raw, _ := json.Marshal(result)
				lastResult = string(raw)
			default:
				return "", fmt.Errorf("unknown step type: %s", stepType)
			}
		}
	}

	if lastResult != "" {
		return lastResult, nil
	}
	raw, _ := json.Marshal(vars)
	return string(raw), nil
}

// resolveExpression resolves variable references in expressions.
func resolveExpression(expr any, vars map[string]any) any {
	switch v := expr.(type) {
	case string:
		if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
			varName := v[2 : len(v)-1]
			if val, ok := vars[varName]; ok {
				return val
			}
		}
		return v
	case map[string]any:
		result := make(map[string]any, len(v))
		for k, val := range v {
			result[k] = resolveExpression(val, vars)
		}
		return result
	case []any:
		result := make([]any, len(v))
		for i, val := range v {
			result[i] = resolveExpression(val, vars)
		}
		return result
	default:
		return expr
	}
}

// executeCall handles a call step with SSRF protection.
func executeCall(ctx context.Context, callDef map[string]any, vars map[string]any) (any, error) {
	fn, _ := callDef["call"].(string)
	args, _ := callDef["args"].(map[string]any)

	switch fn {
	case "sys.log":
		msg := resolveExpression(args["text"], vars)
		log.Printf("[Workflows] sys.log: %v", msg)
		return nil, nil
	case "sys.now":
		return time.Now().UTC().Format(time.RFC3339), nil
	case "http.get", "http.post":
		urlVal := resolveExpression(args["url"], vars)
		urlStr, _ := urlVal.(string)
		if urlStr == "" {
			return nil, fmt.Errorf("call %s: url is required", fn)
		}
		if err := validateCallURL(urlStr); err != nil {
			return nil, fmt.Errorf("call %s: %w", fn, err)
		}
		method := strings.ToUpper(strings.TrimPrefix(fn, "http."))
		req, err := http.NewRequestWithContext(ctx, method, urlStr, nil)
		if err != nil {
			return nil, fmt.Errorf("call %s: %w", fn, err)
		}
		resp, err := noRedirectClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("call %s: %w", fn, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPResponseSize+1))
		if err != nil {
			return nil, fmt.Errorf("call %s: read response: %w", fn, err)
		}
		if len(body) > maxHTTPResponseSize {
			return nil, fmt.Errorf("call %s: response exceeds 1 MiB", fn)
		}
		var respBody any
		if err := json.Unmarshal(body, &respBody); err != nil {
			respBody = string(body)
		}
		return map[string]any{"status": resp.StatusCode, "body": respBody}, nil
	default:
		return nil, fmt.Errorf("unknown call method: %s", fn)
	}
}

// validateCallURL enforces SSRF protection: only http://localhost:8080 is allowed.
func validateCallURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.User != nil {
		return fmt.Errorf("SSRF protection: embedded credentials are not allowed")
	}
	if parsed.Scheme != "http" {
		return fmt.Errorf("SSRF protection: only http://localhost:8080 is allowed, got scheme %q", parsed.Scheme)
	}
	host := parsed.Hostname()
	port := parsed.Port()
	if host != "localhost" && host != "127.0.0.1" {
		return fmt.Errorf("SSRF protection: only http://localhost:8080 is allowed, got host %q", host)
	}
	if port != "8080" {
		return fmt.Errorf("SSRF protection: only http://localhost:8080 is allowed, got port %q", port)
	}
	return nil
}
