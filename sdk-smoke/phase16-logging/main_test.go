package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGeneratedLoggingClientUsesCanonicalSupportedSlice(t *testing.T) {
	const project = "sdk-project"
	sinkExists := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/_minisky/logging/v2/projects/sdk-project/sinks":
			var body map[string]any
			decodeTestJSON(t, r, &body)
			if body["name"] != sinkName || body["destination"] != "file://phase16-errors" ||
				body["filter"] != "severity>=EMERGENCY" {
				t.Fatalf("create sink body=%#v", body)
			}
			sinkExists = true
			_, _ = w.Write([]byte(sinkJSON()))
		case r.Method == http.MethodPost && r.URL.Path == "/_minisky/logging/v2/entries:write":
			var body struct {
				LogName  string            `json:"logName"`
				Labels   map[string]string `json:"labels"`
				Entries  []map[string]any  `json:"entries"`
				Resource map[string]any    `json:"resource"`
			}
			decodeTestJSON(t, r, &body)
			if body.LogName != "projects/sdk-project/logs/"+logID || len(body.Entries) != 3 ||
				body.Labels["phase"] != "16" {
				t.Fatalf("write body=%#v", body)
			}
			if body.Entries[2]["logName"] != "projects/sdk-project-other/logs/"+logID {
				t.Fatalf("cross-project entry=%#v", body.Entries[2])
			}
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodPost && r.URL.Path == "/_minisky/logging/v2/entries:list":
			var body struct {
				ResourceNames []string `json:"resourceNames"`
				Filter        string   `json:"filter"`
				OrderBy       string   `json:"orderBy"`
				PageSize      int64    `json:"pageSize"`
			}
			decodeTestJSON(t, r, &body)
			wantFilter := `severity>=ERROR AND logName="projects/sdk-project/logs/phase16-app" AND resource.type="global"`
			if len(body.ResourceNames) != 1 || body.ResourceNames[0] != "projects/sdk-project" ||
				body.Filter != wantFilter || body.OrderBy != "timestamp asc" || body.PageSize != 10 {
				t.Fatalf("list body=%#v", body)
			}
			_, _ = w.Write([]byte(`{"entries":[{
				"insertId":"phase16-error",
				"timestamp":"2026-07-26T08:01:00Z",
				"severity":"ERROR",
				"textPayload":"phase16 error",
				"logName":"projects/sdk-project/logs/phase16-app",
				"resource":{"type":"global","labels":{"project_id":"sdk-project"}},
				"labels":{"phase":"16","entry":"error","shared":"entry"}
			}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/_minisky/logging/v2/projects/sdk-project/sinks/"+sinkName:
			if !sinkExists {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":{"code":404,"status":"NOT_FOUND","message":"missing"}}`))
				return
			}
			_, _ = w.Write([]byte(sinkJSON()))
		case r.Method == http.MethodGet && r.URL.Path == "/_minisky/logging/v2/projects/sdk-project/sinks":
			_, _ = w.Write([]byte(`{"sinks":[` + sinkJSON() + `]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/_minisky/logging/v2/projects/sdk-project/sinks/"+sinkName:
			sinkExists = false
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	if err := seed(ctx, server.URL, project, project+"-other"); err != nil {
		t.Fatal(err)
	}
	if err := verify(ctx, server.URL, project); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(ctx, server.URL, project); err != nil {
		t.Fatal(err)
	}
	if err := verifyCleanup(ctx, server.URL, project); err != nil {
		t.Fatal(err)
	}
}

func decodeTestJSON(t *testing.T, request *http.Request, target any) {
	t.Helper()
	if err := json.NewDecoder(request.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func sinkJSON() string {
	return `{"name":"phase16-errors","destination":"file://phase16-errors","filter":"severity>=EMERGENCY","description":"Phase 16 restart SDK smoke"}`
}
