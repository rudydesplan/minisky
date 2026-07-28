package privateca

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

type memoryStore struct {
	mu      sync.Mutex
	payload []byte
	saveErr error
}

func (s *memoryStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.payload, target)
}

func (s *memoryStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	payload, err := json.Marshal(value)
	if err == nil {
		s.payload = payload
	}
	return err
}

func TestIssuePersistsCertificateWithoutPrivateKeyMaterial(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	csr := testCSR(t, "service.local")
	cert, err := api.Issue("projects/p/locations/us/caPools/local", "leaf", csr, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if cert.PEMCertificate == "" || cert.Name == "" {
		t.Fatalf("certificate = %#v", cert)
	}
	store.mu.Lock()
	payload := append([]byte(nil), store.payload...)
	store.mu.Unlock()
	if bytes.Contains(payload, []byte("PRIVATE KEY")) || bytes.Contains(payload, csr) {
		t.Fatalf("state leaked key/CSR material: %s", payload)
	}

	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := restarted.Get(cert.Name)
	if !ok || got.PEMCertificate != cert.PEMCertificate {
		t.Fatalf("restarted certificate = %#v, ok=%v", got, ok)
	}
}

func TestIssueFailsClosedOnAuthorizationAndSaveFailure(t *testing.T) {
	denied, err := NewAPIWithStore(nil, AuthorizerFunc(func(string, string) error {
		return errors.New("denied")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Issue("projects/p/locations/us/caPools/local", "leaf", testCSR(t, "service.local"), time.Hour); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("authorization error = %v", err)
	}

	store := &memoryStore{saveErr: errors.New("disk full")}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.Issue("projects/p/locations/us/caPools/local", "leaf", testCSR(t, "service.local"), time.Hour); err == nil {
		t.Fatal("expected save failure")
	}
	if _, ok := api.Get("projects/p/locations/us/caPools/local/certificates/leaf"); ok {
		t.Fatal("failed issue remained in memory")
	}
}

func TestCreateCertificateUsesOptionalQueryIDAndDirectCertificateBody(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"pemCsr":   string(testCSR(t, "service.local")),
		"lifetime": "3600s",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/p/locations/us/caPools/local/certificates?certificateId=leaf",
		strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var certificate Certificate
	if err := json.Unmarshal(response.Body.Bytes(), &certificate); err != nil {
		t.Fatal(err)
	}
	if certificate.Name != "projects/p/locations/us/caPools/local/certificates/leaf" {
		t.Fatalf("certificate name = %q", certificate.Name)
	}
}

func TestConcurrentIssueHasAtomicIdentity(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	csr := testCSR(t, "service.local")
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := api.Issue("projects/p/locations/us/caPools/local", "same", csr, time.Hour)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful issues = %d, want 1", successes)
	}
}

func TestRevokePersistsWithoutExposingKeyMaterial(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := api.Issue(
		"projects/p/locations/us/caPools/local", "revoked", testCSR(t, "revoked.local"), time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.Revoke(certificate.Name, "KEY_COMPROMISE"); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := restarted.Get(certificate.Name)
	if !ok || !got.Revoked || got.RevocationReason != "KEY_COMPROMISE" {
		t.Fatalf("revoked certificate = %#v, ok=%v", got, ok)
	}
	store.mu.Lock()
	payload := append([]byte(nil), store.payload...)
	store.mu.Unlock()
	if bytes.Contains(payload, []byte("PRIVATE KEY")) {
		t.Fatalf("state leaked private key material: %s", payload)
	}
}

func TestRevokeCertificateRequestUpdatesPersistedDecisionPath(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := api.Issue(
		"projects/p/locations/us/caPools/local", "revoked-http", testCSR(t, "revoked.local"), time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/"+certificate.Name+":revoke",
		strings.NewReader(`{"reason":"KEY_COMPROMISE"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var revoked Certificate
	if err := json.Unmarshal(response.Body.Bytes(), &revoked); err != nil {
		t.Fatal(err)
	}
	if !revoked.Revoked || revoked.RevocationReason != "KEY_COMPROMISE" {
		t.Fatalf("revoked certificate = %#v", revoked)
	}

	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := restarted.Get(certificate.Name); !ok || !got.Revoked {
		t.Fatalf("persisted certificate = %#v, ok=%v", got, ok)
	}
}

func TestRevokeCertificateRejectsUnsupportedReason(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := api.Issue(
		"projects/p/locations/us/caPools/local", "invalid-reason", testCSR(t, "revoked.local"), time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/"+certificate.Name+":revoke",
		strings.NewReader(`{"reason":"NOT_A_REASON"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got, ok := api.Get(certificate.Name); !ok || got.Revoked {
		t.Fatalf("certificate = %#v, ok=%v", got, ok)
	}
}

func TestCorruptPersistedCertificateIsRejected(t *testing.T) {
	store := &memoryStore{}
	store.payload, _ = json.Marshal(metadata{Certificates: map[string]*Certificate{
		"projects/p/locations/us/caPools/local/certificates/leaf": {
			Name:           "projects/p/locations/us/caPools/local/certificates/leaf",
			PEMCertificate: "-----BEGIN PRIVATE KEY-----\nleak\n-----END PRIVATE KEY-----",
		},
	}})
	if _, err := NewAPIWithStore(store, AllowAllAuthorizer{}); err == nil {
		t.Fatal("expected persisted private key material to be rejected")
	}
}

func testCSR(t *testing.T, commonName string) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: commonName},
		DNSNames: []string{commonName},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: request})
}
