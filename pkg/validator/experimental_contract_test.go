package validator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"minisky/pkg/evidence"
)

func TestExperimentalMutationContractsRejectMissingRequiredInputs(t *testing.T) {
	inventory, err := evidence.Phase18To25()
	if err != nil {
		t.Fatal(err)
	}
	experimental := make(map[string]bool, len(inventory))
	for _, entry := range inventory {
		experimental[entry.Domain] = true
	}

	type contractCase struct {
		domain string
		rule   MethodSchema
	}
	var contracts []contractCase
	coveredDomains := make(map[string]bool)
	for _, service := range embeddedRules {
		if !experimental[service.Domain] {
			continue
		}
		for _, rule := range service.Methods {
			if !isMutationMethod(rule.HTTPMethod) {
				continue
			}
			contracts = append(contracts, contractCase{domain: service.Domain, rule: rule})
			coveredDomains[service.Domain] = true
		}
	}

	const expectedContracts = 34
	if len(contracts) != expectedContracts {
		t.Fatalf("experimental mutation contracts = %d, want %d", len(contracts), expectedContracts)
	}
	for _, domain := range []string{
		"accesscontextmanager.googleapis.com",
		"alloydb.googleapis.com",
		"apigateway.googleapis.com",
		"batch.googleapis.com",
		"binaryauthorization.googleapis.com",
		"clouddeploy.googleapis.com",
		"clouderrorreporting.googleapis.com",
		"cloudprofiler.googleapis.com",
		"cloudtrace.googleapis.com",
		"composer.googleapis.com",
		"dataflow.googleapis.com",
		"dataform.googleapis.com",
		"dialogflow.googleapis.com",
		"dlp.googleapis.com",
		"documentai.googleapis.com",
		"eventarc.googleapis.com",
		"file.googleapis.com",
		"identityplatform.googleapis.com",
		"managedkafka.googleapis.com",
		"networksecurity.googleapis.com",
		"networkservices.googleapis.com",
		"orgpolicy.googleapis.com",
		"privateca.googleapis.com",
		"servicedirectory.googleapis.com",
		"storagetransfer.googleapis.com",
		"translate.googleapis.com",
		"vision.googleapis.com",
		"workflows.googleapis.com",
	} {
		if !coveredDomains[domain] {
			t.Errorf("%s has no executable experimental mutation contract", domain)
		}
	}

	validator := NewValidator()
	for _, contract := range contracts {
		contract := contract
		name := contract.domain + " " + contract.rule.HTTPMethod + " " + contract.rule.PathGlob
		t.Run(name, func(t *testing.T) {
			path := materializeValidatorGlob(contract.rule.PathGlob)
			query := make(url.Values, len(contract.rule.RequiredQuery))
			for _, field := range contract.rule.RequiredQuery {
				query.Set(field, "evidence")
			}
			validBody := requiredValidatorBody(contract.rule.RequiredBody)
			assertExperimentalContractResult(t, validator, contract.domain, contract.rule.HTTPMethod,
				path, query, validBody, true, "")

			for _, field := range contract.rule.RequiredQuery {
				invalidQuery := cloneValidatorQuery(query)
				invalidQuery.Del(field)
				assertExperimentalContractResult(t, validator, contract.domain, contract.rule.HTTPMethod,
					path, invalidQuery, validBody, false, field)
			}
			for _, field := range contract.rule.RequiredBody {
				invalidBody := cloneValidatorBody(t, validBody)
				deleteValidatorField(invalidBody, field.Path)
				assertExperimentalContractResult(t, validator, contract.domain, contract.rule.HTTPMethod,
					path, query, invalidBody, false, field.Path)
			}
		})
	}
}

func TestExperimentalDecisionActionContracts(t *testing.T) {
	tests := []struct {
		name, domain, path, validBody, invalidBody, wantMessage string
	}{
		{
			name: "private ca revoke", domain: "privateca.googleapis.com",
			path:        "/v1/projects/demo/locations/us-central1/caPools/pool/certificates/cert:revoke",
			validBody:   `{"reason":"KEY_COMPROMISE"}`,
			invalidBody: `{}`,
			wantMessage: "reason",
		},
		{
			name: "binary authorization evaluate", domain: "binaryauthorization.googleapis.com",
			path:        "/v1/projects/demo/policy:evaluate",
			validBody:   `{"image":"us-docker.pkg.dev/demo/releases/app@sha256:abc"}`,
			invalidBody: `{}`,
			wantMessage: "image",
		},
		{
			name: "access context check", domain: "accesscontextmanager.googleapis.com",
			path:        "/v1/accessPolicies/1:checkAccess",
			validBody:   `{"project":"projects/demo","service":"storage.googleapis.com"}`,
			invalidBody: `{"project":"projects/demo"}`,
			wantMessage: "service",
		},
		{
			name: "network security evaluate", domain: "networksecurity.googleapis.com",
			path:        "/v1/projects/demo/locations/global/authorizationPolicies:evaluate",
			validBody:   `{"project":"demo","location":"global"}`,
			invalidBody: `{"project":"demo"}`,
			wantMessage: "location",
		},
		{
			name: "service mesh resolve", domain: "networkservices.googleapis.com",
			path:        "/v1/projects/demo/locations/global/httpRoutes:resolve",
			validBody:   `{"project":"demo","location":"global","host":"api.local","path":"/v1/items"}`,
			invalidBody: `{"project":"demo","location":"global","host":"api.local"}`,
			wantMessage: "path",
		},
	}

	validator := NewValidator()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			valid := newJSONRequest(t, http.MethodPost, test.path, test.validBody)
			validResponse := httptest.NewRecorder()
			if !validator.ValidateRequestForDomain(validResponse, valid, test.domain) {
				t.Fatalf("valid action rejected: status=%d body=%s", validResponse.Code, validResponse.Body.String())
			}

			invalid := newJSONRequest(t, http.MethodPost, test.path, test.invalidBody)
			invalidResponse := httptest.NewRecorder()
			if validator.ValidateRequestForDomain(invalidResponse, invalid, test.domain) {
				t.Fatal("invalid action passed validation")
			}
			assertInvalidArgument(t, invalidResponse, test.wantMessage)
		})
	}
}

func assertExperimentalContractResult(
	t *testing.T,
	validator *Validator,
	domain, method, path string,
	query url.Values,
	body map[string]any,
	wantValid bool,
	wantMessage string,
) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	valid := validator.ValidateRequestForDomain(response, request, domain)
	if valid != wantValid {
		t.Fatalf("valid=%v, want %v; status=%d body=%s", valid, wantValid, response.Code, response.Body.String())
	}
	if !wantValid {
		assertInvalidArgument(t, response, wantMessage)
	}
}

func materializeValidatorGlob(glob string) string {
	return strings.ReplaceAll(glob, "*", "evidence")
}

func requiredValidatorBody(fields []BodyField) map[string]any {
	body := make(map[string]any)
	for _, field := range fields {
		var value any = "evidence"
		switch field.Type {
		case "array":
			value = []any{map[string]any{}}
		case "object":
			value = map[string]any{}
		case "integer":
			value = 1
		case "boolean":
			value = true
		}
		setValidatorField(body, field.Path, value)
	}
	return body
}

func setValidatorField(body map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	current := body
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}

func deleteValidatorField(body map[string]any, path string) {
	parts := strings.Split(path, ".")
	current := body
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, parts[len(parts)-1])
}

func cloneValidatorBody(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	var clone map[string]any
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func cloneValidatorQuery(query url.Values) url.Values {
	clone := make(url.Values, len(query))
	for key, values := range query {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}
