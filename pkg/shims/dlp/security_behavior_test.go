package dlp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInspectContentUsesDeterministicBoundedDetectors(t *testing.T) {
	api := newAPI(nil)
	body := `{"item":{"value":"Contact a@example.com or 4111 1111 1111 1111."},"inspectConfig":{"infoTypes":[{"name":"EMAIL_ADDRESS"},{"name":"CREDIT_CARD_NUMBER"}],"includeQuote":true}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p/locations/global/content:inspect", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Result struct {
			Findings []struct {
				InfoType struct {
					Name string `json:"name"`
				} `json:"infoType"`
				Quote string `json:"quote"`
			} `json:"findings"`
		} `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Result.Findings) != 2 {
		t.Fatalf("findings = %#v", response.Result.Findings)
	}
	if response.Result.Findings[0].InfoType.Name != "EMAIL_ADDRESS" ||
		response.Result.Findings[0].Quote != "a@example.com" {
		t.Fatalf("first finding = %#v", response.Result.Findings[0])
	}
	if response.Result.Findings[1].InfoType.Name != "CREDIT_CARD_NUMBER" {
		t.Fatalf("second finding = %#v", response.Result.Findings[1])
	}
}

func TestDeidentifyContentAppliesReplaceConfigOnlyToFindings(t *testing.T) {
	api := newAPI(nil)
	body := `{
		"item":{"value":"mail a@example.com; keep this"},
		"inspectConfig":{"infoTypes":[{"name":"EMAIL_ADDRESS"}]},
		"deidentifyConfig":{"infoTypeTransformations":{"transformations":[{
			"primitiveTransformation":{"replaceConfig":{"newValue":{"stringValue":"[EMAIL]"}}}
		}]}}
	}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p/locations/global/content:deidentify", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Item struct {
			Value string `json:"value"`
		} `json:"item"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Item.Value != "mail [EMAIL]; keep this" {
		t.Fatalf("value = %q", response.Item.Value)
	}
}
