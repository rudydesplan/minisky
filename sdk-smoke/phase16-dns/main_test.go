package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func TestGeneratedDNSClientUsesCanonicalSupportedSlice(t *testing.T) {
	const project = "sdk-project"
	zoneDNS := dnsNameForProject(project)
	recordName := "app." + zoneDNS
	var mu sync.Mutex
	zoneExists := false
	recordExists := false
	sawCaseVariantGet := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("alt") != "json" || r.URL.Query().Get("prettyPrint") != "false" {
			t.Errorf("query=%q for %s %s", r.URL.RawQuery, r.Method, r.URL.Path)
		}
		zoneCollection := "/_minisky/dns/dns/v1/projects/sdk-project/managedZones"
		zoneItem := zoneCollection + "/" + zoneName
		rrCollection := zoneItem + "/rrsets"
		rrItem := rrCollection + "/" + recordName + "/A"
		rrCaseVariantItem := rrCollection + "/" + strings.ToUpper(recordName) + "/a"
		switch {
		case r.Method == http.MethodPost && r.URL.Path == zoneCollection:
			assertJSONKeys(t, r, "dnsName", "name", "visibility")
			zoneExists = true
			writeTestJSON(w, managedZoneJSON(project))
		case r.Method == http.MethodGet && r.URL.Path == zoneItem:
			if !zoneExists {
				writeTestNotFound(w)
				return
			}
			writeTestJSON(w, managedZoneJSON(project))
		case r.Method == http.MethodGet && r.URL.Path == zoneCollection:
			zones := []any{}
			if zoneExists {
				zones = append(zones, managedZoneJSON(project))
			}
			writeTestJSON(w, map[string]any{"kind": "dns#managedZonesListResponse", "managedZones": zones})
		case r.Method == http.MethodPost && r.URL.Path == rrCollection:
			assertJSONKeys(t, r, "name", "rrdatas", "ttl", "type")
			recordExists = true
			writeTestJSON(w, recordJSON(project))
		case r.Method == http.MethodGet && (r.URL.Path == rrCaseVariantItem || r.URL.Path == rrItem):
			if r.URL.Path == rrCaseVariantItem {
				sawCaseVariantGet = true
			}
			if !recordExists {
				writeTestNotFound(w)
				return
			}
			writeTestJSON(w, recordJSON(project))
		case r.Method == http.MethodGet && r.URL.Path == rrCollection:
			if r.URL.Query().Get("name") != recordName || r.URL.Query().Get("type") != "A" {
				t.Errorf("RRSet list query=%q", r.URL.RawQuery)
			}
			rrsets := []any{}
			if recordExists {
				rrsets = append(rrsets, recordJSON(project))
			}
			writeTestJSON(w, map[string]any{"kind": "dns#resourceRecordSetsListResponse", "rrsets": rrsets})
		case r.Method == http.MethodDelete && r.URL.Path == rrItem:
			recordExists = false
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == zoneItem:
			zoneExists = false
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			writeTestNotFound(w)
		}
	}))
	defer server.Close()

	dnsAddr, closeDNS := startTestDNS(t, recordName, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return zoneExists && recordExists
	})
	defer closeDNS()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := seed(ctx, server.URL, project); err != nil {
		t.Fatal(err)
	}
	if err := verify(ctx, server.URL, project, dnsAddr); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(ctx, server.URL, project); err != nil {
		t.Fatal(err)
	}
	if err := verifyCleanup(ctx, server.URL, project, dnsAddr); err != nil {
		t.Fatal(err)
	}
	if !sawCaseVariantGet {
		t.Fatal("generated item GET did not preserve the case-variant request path")
	}
}

func assertJSONKeys(t *testing.T, r *http.Request, expected ...string) {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body) != len(expected) {
		t.Errorf("body keys=%v want=%v", body, expected)
		return
	}
	for _, key := range expected {
		if _, ok := body[key]; !ok {
			t.Errorf("body missing %q: %#v", key, body)
		}
	}
}

func managedZoneJSON(project string) map[string]any {
	return map[string]any{
		"kind":       "dns#managedZone",
		"name":       zoneName,
		"dnsName":    dnsNameForProject(project),
		"visibility": "public",
	}
}

func recordJSON(project string) map[string]any {
	return map[string]any{
		"kind":    "dns#resourceRecordSet",
		"name":    "app." + dnsNameForProject(project),
		"type":    "A",
		"ttl":     60,
		"rrdatas": []string{recordIP},
	}
}

func writeTestJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}

func writeTestNotFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	writeTestJSON(w, map[string]any{
		"error": map[string]any{"code": 404, "status": "NOT_FOUND", "message": "missing"},
	})
}

func startTestDNS(t *testing.T, expectedName string, exists func() bool) (string, func()) {
	t.Helper()
	connection, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 1500)
		for {
			size, peer, err := connection.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if err := query.Unpack(buffer[:size]); err != nil || len(query.Questions) != 1 {
				continue
			}
			question := query.Questions[0]
			response := dnsmessage.Message{
				Header: dnsmessage.Header{
					ID: query.Header.ID, Response: true, Authoritative: true,
					RCode: dnsmessage.RCodeNameError,
				},
				Questions: query.Questions,
			}
			if question.Name.String() == expectedName && question.Type == dnsmessage.TypeA && exists() {
				response.Header.RCode = dnsmessage.RCodeSuccess
				response.Answers = []dnsmessage.Resource{{
					Header: dnsmessage.ResourceHeader{
						Name: question.Name, Type: dnsmessage.TypeA,
						Class: dnsmessage.ClassINET, TTL: 60,
					},
					Body: &dnsmessage.AResource{A: [4]byte{192, 0, 2, 16}},
				}}
			}
			packet, err := response.Pack()
			if err == nil {
				_, _ = connection.WriteToUDP(packet, peer)
			}
		}
	}()
	return connection.LocalAddr().String(), func() {
		_ = connection.Close()
		<-done
	}
}

func TestValidateInputs(t *testing.T) {
	for _, test := range []struct {
		name, gateway, project, address string
	}{
		{"gateway", "://bad", "valid-project", "127.0.0.1:1053"},
		{"project path", "http://127.0.0.1", "bad/project", "127.0.0.1:1053"},
		{"project DNS", "http://127.0.0.1", "Bad_Project", "127.0.0.1:1053"},
		{"address", "http://127.0.0.1", "valid-project", "not-an-address"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateInputs(test.gateway, test.project, test.address); err == nil {
				t.Fatal("invalid inputs accepted")
			}
		})
	}
	if parsed, err := url.Parse("http://127.0.0.1"); err != nil || parsed.Host == "" {
		t.Fatal("test URL is invalid")
	}
}
