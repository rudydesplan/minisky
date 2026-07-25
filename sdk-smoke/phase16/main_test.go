package main

import (
	"strings"
	"testing"
)

func TestPromQLURLUsesCanonicalMonitoringEndpoint(t *testing.T) {
	got := promQLURL("http://127.0.0.1:8080/", "test-project", "custom.googleapis.com/a_b-c.d")
	if !strings.HasPrefix(got, "http://127.0.0.1:8080/_minisky/monitoring/v1/projects/test-project/location/global/prometheus/api/v1/query?") {
		t.Fatalf("URL = %q", got)
	}
	if !strings.Contains(got, "query=%7B__name__%3D%22custom.googleapis.com%2Fa_b-c.d%22%7D") {
		t.Fatalf("URL query = %q", got)
	}
}

func TestPersistedSample(t *testing.T) {
	body := []byte(`{
		"status":"success",
		"data":{"resultType":"vector","result":[{
			"metric":{"__name__":"custom.googleapis.com/test","source":"phase16"},
			"value":[1800000000,"42.125"]
		}]}
	}`)
	got, err := persistedSample(body, "custom.googleapis.com/test")
	if err != nil {
		t.Fatal(err)
	}
	if got != "42.125" {
		t.Fatalf("sample=%q", got)
	}
}

func TestPersistedSampleRejectsUnexpectedResult(t *testing.T) {
	_, err := persistedSample([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`),
		"custom.googleapis.com/test")
	if err == nil {
		t.Fatal("expected empty result error")
	}
}
