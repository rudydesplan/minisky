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
	result, err := runWorkflow(ctx, sourceContents, argument)

	api.mu.Lock()
	exec := api.executions[execName]
	if exec != nil && exec.State == "ACTIVE" {
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
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		log.Printf("[Workflows] persist execution outcome failed: %v", err)
	}
}

// CreateExecutionFromEvent implements the Eventarc WorkflowsExecutor interface.
func (api *API) CreateExecutionFromEvent(workflowName, eventPayload string) error {
	api.mu.RLock()
	wf := api.workflows[workflowName]
	api.mu.RUnlock()
	if wf == nil {
		return fmt.Errorf("workflow not found: %s", workflowName)
	}

	execID := fmt.Sprintf("event-%d", time.Now().UnixNano())
	execName := fmt.Sprintf("%s/executions/%s", workflowName, execID)
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
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.executions, execName)
		api.mu.Unlock()
		api.compensateState(err)
		return fmt.Errorf("persist event execution: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	api.startExecution(execName, cancel)
	go api.ExecuteWorkflow(ctx, execName, wf.SourceContents, eventPayload)
	return nil
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
