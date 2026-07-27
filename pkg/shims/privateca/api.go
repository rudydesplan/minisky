// Package privateca provides a bounded local Certificate Authority Service shim.
package privateca

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const privateCAStateEntry = "privateca/metadata"

var (
	ErrAlreadyExists     = errors.New("certificate already exists")
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrNotFound          = errors.New("certificate not found")
	ErrPermissionDenied  = errors.New("permission denied")
	certificateIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	parentPattern        = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/caPools/[^/]+$`)
)

func init() {
	state.MustRegisterEntryValidator(privateCAStateEntry, state.StrictEntryValidator[metadata](nil))
	registry.Register("privateca.googleapis.com", func(*registry.Context) http.Handler {
		return NewAPI()
	})
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type Authorizer interface {
	Authorize(action, resource string) error
}

type AuthorizerFunc func(action, resource string) error

func (authorize AuthorizerFunc) Authorize(action, resource string) error {
	return authorize(action, resource)
}

type AllowAllAuthorizer struct{}

func (AllowAllAuthorizer) Authorize(string, string) error { return nil }

type Certificate struct {
	Name             string `json:"name"`
	PEMCertificate   string `json:"pemCertificate"`
	SerialNumber     string `json:"serialNumber"`
	Issuer           string `json:"issuer"`
	CreateTime       string `json:"createTime"`
	ExpireTime       string `json:"expireTime"`
	Revoked          bool   `json:"revoked,omitempty"`
	RevocationReason string `json:"revocationReason,omitempty"`
	RevocationTime   string `json:"revocationTime,omitempty"`
}

type metadata struct {
	Certificates map[string]*Certificate `json:"certificates"`
}

type API struct {
	mu                sync.RWMutex
	persistMu         sync.Mutex
	store             stateStore
	authorizer        Authorizer
	initializationErr error
	certificates      map[string]*Certificate
	caKey             *rsa.PrivateKey
	caCert            *x509.Certificate
	caPEM             string
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	guarded := state.NewGuardedEntryStore(store, err)
	api, loadErr := NewAPIWithStore(guarded, AllowAllAuthorizer{})
	if loadErr == nil {
		return api
	}
	api, _ = NewAPIWithStore(nil, AllowAllAuthorizer{})
	api.store = guarded
	api.initializationErr = loadErr
	return api
}

func NewAPIWithStore(store stateStore, authorizer Authorizer) (*API, error) {
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer is required", ErrInvalidArgument)
	}
	key, certificate, certificatePEM, err := newEphemeralCA()
	if err != nil {
		return nil, fmt.Errorf("create ephemeral local CA: %w", err)
	}
	api := &API{
		store: store, authorizer: authorizer, certificates: make(map[string]*Certificate),
		caKey: key, caCert: certificate, caPEM: certificatePEM,
	}
	if store == nil {
		return api, nil
	}
	var saved metadata
	if err := store.Load(privateCAStateEntry, &saved); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load certificate metadata: %w", err)
	}
	for name, certificate := range saved.Certificates {
		if certificate == nil || certificate.Name != name || strings.Contains(certificate.PEMCertificate, "PRIVATE KEY") {
			return nil, fmt.Errorf("invalid certificate metadata")
		}
		api.certificates[name] = cloneCertificate(certificate)
	}
	return api, nil
}

func (api *API) Issue(parent, certificateID string, csrPEM []byte, lifetime time.Duration) (*Certificate, error) {
	if api.initializationErr != nil {
		return nil, fmt.Errorf("certificate persistence unavailable: %w", api.initializationErr)
	}
	if !parentPattern.MatchString(parent) || !certificateIDPattern.MatchString(certificateID) ||
		lifetime < time.Minute || lifetime > 365*24*time.Hour {
		return nil, ErrInvalidArgument
	}
	name := parent + "/certificates/" + certificateID
	if err := api.authorizer.Authorize("privateca.certificates.create", name); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}
	request, err := parseCSR(csrPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	now := time.Now().UTC()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      request.Subject,
		DNSNames:     append([]string(nil), request.DNSNames...),
		IPAddresses:  append([]net.IP(nil), request.IPAddresses...),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(lifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, api.caCert, request.PublicKey, api.caKey)
	if err != nil {
		return nil, fmt.Errorf("issue certificate: %w", err)
	}
	certificate := &Certificate{
		Name:           name,
		PEMCertificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		SerialNumber:   serial.String(),
		Issuer:         api.caCert.Subject.String(),
		CreateTime:     now.Format(time.RFC3339Nano),
		ExpireTime:     template.NotAfter.Format(time.RFC3339Nano),
	}
	api.mu.Lock()
	if _, exists := api.certificates[name]; exists {
		api.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	api.certificates[name] = certificate
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.certificates, name)
		api.mu.Unlock()
		return nil, fmt.Errorf("persist certificate metadata: %w", err)
	}
	return cloneCertificate(certificate), nil
}

func (api *API) Revoke(name, reason string) error {
	if api.initializationErr != nil {
		return fmt.Errorf("certificate persistence unavailable: %w", api.initializationErr)
	}
	if err := api.authorizer.Authorize("privateca.certificates.revoke", name); err != nil {
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}
	api.mu.Lock()
	certificate := api.certificates[name]
	if certificate == nil {
		api.mu.Unlock()
		return ErrNotFound
	}
	before := cloneCertificate(certificate)
	certificate.Revoked = true
	certificate.RevocationReason = reason
	certificate.RevocationTime = time.Now().UTC().Format(time.RFC3339Nano)
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		api.certificates[name] = before
		api.mu.Unlock()
		return fmt.Errorf("persist revocation: %w", err)
	}
	return nil
}

func (api *API) Get(name string) (*Certificate, bool) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	certificate := api.certificates[name]
	return cloneCertificate(certificate), certificate != nil
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	snapshot := make(map[string]*Certificate, len(api.certificates))
	for name, certificate := range api.certificates {
		snapshot[name] = cloneCertificate(certificate)
	}
	api.mu.RUnlock()
	return api.store.Save(privateCAStateEntry, metadata{Certificates: snapshot})
}

func (api *API) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/certificates") {
		request.Body = http.MaxBytesReader(w, request.Body, 1<<20)
		var body struct {
			PEMCSR   string `json:"pemCsr"`
			Lifetime string `json:"lifetime"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid request")
			return
		}
		lifetime, err := time.ParseDuration(body.Lifetime)
		if err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid lifetime")
			return
		}
		parent := strings.TrimPrefix(strings.TrimSuffix(request.URL.Path, "/certificates"), "/v1/")
		certificateID := request.URL.Query().Get("certificateId")
		if certificateID == "" {
			generated := make([]byte, 8)
			if _, err := rand.Read(generated); err != nil {
				writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to generate certificate id")
				return
			}
			certificateID = fmt.Sprintf("certificate-%x", generated)
		}
		certificate, err := api.Issue(parent, certificateID, []byte(body.PEMCSR), lifetime)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		_ = json.NewEncoder(w).Encode(certificate)
		return
	}
	writeError(w, 501, "UNIMPLEMENTED", "Certificate Authority Service operation is not implemented")
}

func parseCSR(encoded []byte) (*x509.CertificateRequest, error) {
	block, trailing := pem.Decode(encoded)
	if block == nil || block.Type != "CERTIFICATE REQUEST" || len(strings.TrimSpace(string(trailing))) != 0 {
		return nil, errors.New("PEM CSR is required")
	}
	request, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, err
	}
	if err := request.CheckSignature(); err != nil {
		return nil, err
	}
	return request, nil
}

func newEphemeralCA() (*rsa.PrivateKey, *x509.Certificate, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, "", err
	}
	now := time.Now().UTC()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, "", err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "MiniSky Ephemeral Local CA"},
		NotBefore:    now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, "", err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, "", err
	}
	return key, certificate, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), nil
}

func cloneCertificate(certificate *Certificate) *Certificate {
	if certificate == nil {
		return nil
	}
	clone := *certificate
	return &clone
}

func writeAPIError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, 400, "INVALID_ARGUMENT", err.Error())
	case errors.Is(err, ErrPermissionDenied):
		writeError(w, 403, "PERMISSION_DENIED", "permission denied")
	case errors.Is(err, ErrAlreadyExists):
		writeError(w, 409, "ALREADY_EXISTS", err.Error())
	case errors.Is(err, ErrNotFound):
		writeError(w, 404, "NOT_FOUND", err.Error())
	default:
		writeError(w, 500, "INTERNAL", "local CA operation failed")
	}
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": status, "details": []any{}},
	})
}
