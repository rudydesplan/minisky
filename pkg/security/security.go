// Package security implements cryptographically real, local-only credential
// and transport primitives. It deliberately does not model production Google
// trust roots, token introspection, certificate issuance, or key custody.
package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidToken = errors.New("invalid local token")
	ErrExpired      = errors.New("local token expired")
	ErrAudience     = errors.New("local token audience mismatch")
	ErrScope        = errors.New("local token scope denied")
)

const (
	maxTokenLifetime = time.Hour
	secretFileName   = "credential-hmac.key"
)

var issuerFileMu sync.Mutex

type Claims struct {
	Subject   string    `json:"sub"`
	Audience  string    `json:"aud,omitempty"`
	Scopes    []string  `json:"scope,omitempty"`
	IssuedAt  time.Time `json:"iat"`
	ExpiresAt time.Time `json:"exp"`
	TokenID   string    `json:"jti"`
}

type TokenRequest struct {
	Subject  string
	Audience string
	Scopes   []string
	Lifetime time.Duration
}

type VerifyOptions struct {
	Audience      string
	RequiredScope string
}

type Issuer struct {
	key []byte
	now func() time.Time
}

func NewIssuer(key []byte, now func() time.Time) *Issuer {
	keyCopy := append([]byte(nil), key...)
	if now == nil {
		now = time.Now
	}
	return &Issuer{key: keyCopy, now: now}
}

// LoadIssuer loads or creates the profile-local signing secret. The secret is
// intentionally outside portable state exports.
func LoadIssuer(profileDir string) (*Issuer, error) {
	issuerFileMu.Lock()
	defer issuerFileMu.Unlock()
	if strings.TrimSpace(profileDir) == "" {
		return nil, errors.New("profile directory is required")
	}
	dir := filepath.Join(profileDir, "security")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create security directory: %w", err)
	}
	if err := requirePrivateDirectory(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, secretFileName)
	key, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("generate credential key: %w", err)
		}
		if err := writeExclusive(path, key, 0o600); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return nil, err
			}
			key, err = os.ReadFile(path)
		} else {
			err = nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("read credential key: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("credential key %s must not be accessible by group or others", path)
	}
	if len(key) < 32 {
		return nil, errors.New("credential key is too short")
	}
	return NewIssuer(key, time.Now), nil
}

func (i *Issuer) Issue(request TokenRequest) (string, Claims, error) {
	if i == nil || len(i.key) < 32 || strings.TrimSpace(request.Subject) == "" {
		return "", Claims{}, errors.New("valid issuer and subject are required")
	}
	lifetime := request.Lifetime
	if lifetime == 0 {
		lifetime = maxTokenLifetime
	}
	if lifetime <= 0 || lifetime > maxTokenLifetime {
		return "", Claims{}, fmt.Errorf("token lifetime must be between 1ns and %s", maxTokenLifetime)
	}
	now := i.now().UTC()
	tokenIDBytes := make([]byte, 16)
	if _, err := rand.Read(tokenIDBytes); err != nil {
		return "", Claims{}, err
	}
	claims := Claims{
		Subject:   strings.TrimSpace(request.Subject),
		Audience:  strings.TrimSpace(request.Audience),
		Scopes:    normalizedScopes(request.Scopes),
		IssuedAt:  now,
		ExpiresAt: now.Add(lifetime),
		TokenID:   base64.RawURLEncoding.EncodeToString(tokenIDBytes),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", Claims{}, err
	}
	signature := i.sign(payload)
	return "ms1." + base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature), claims, nil
}

func (i *Issuer) Verify(token string, options VerifyOptions) (Claims, error) {
	if i == nil {
		return Claims{}, ErrInvalidToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "ms1" {
		return Claims{}, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(signature, i.sign(payload)) {
		return Claims{}, ErrInvalidToken
	}
	var claims Claims
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil || claims.Subject == "" || claims.ExpiresAt.IsZero() {
		return Claims{}, ErrInvalidToken
	}
	if !i.now().Before(claims.ExpiresAt) {
		return Claims{}, ErrExpired
	}
	if options.Audience != "" && claims.Audience != options.Audience {
		return Claims{}, ErrAudience
	}
	if options.RequiredScope != "" && !hasScope(claims.Scopes, options.RequiredScope) {
		return Claims{}, ErrScope
	}
	return claims, nil
}

func (i *Issuer) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, i.key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

type TLSMode string

const (
	TLSDisabled TLSMode = ""
	TLSAuto     TLSMode = "auto"
	TLSFiles    TLSMode = "files"
)

type TLSOptions struct {
	Mode       TLSMode
	ProfileDir string
	CertFile   string
	KeyFile    string
	ClientCA   string
	ServerName string
}

type TLSDiagnostics struct {
	Enabled         bool
	AutoGenerated   bool
	ClientCAEnabled bool
	CertificateFile string
	KeyFile         string
}

func PrepareTLS(options TLSOptions) (*tls.Config, TLSDiagnostics, error) {
	var diagnostics TLSDiagnostics
	if options.Mode == TLSDisabled && options.CertFile == "" && options.KeyFile == "" && options.ClientCA == "" {
		return nil, diagnostics, nil
	}
	if options.Mode == TLSDisabled {
		options.Mode = TLSFiles
	}
	if options.Mode != TLSAuto && options.Mode != TLSFiles {
		return nil, diagnostics, fmt.Errorf("unsupported TLS mode %q", options.Mode)
	}
	if options.Mode == TLSAuto {
		if options.CertFile != "" || options.KeyFile != "" {
			return nil, diagnostics, errors.New("auto TLS cannot be combined with certificate paths")
		}
		cert, key, err := ensureLocalCertificate(options.ProfileDir, options.ServerName)
		if err != nil {
			return nil, diagnostics, err
		}
		options.CertFile, options.KeyFile = cert, key
		diagnostics.AutoGenerated = true
	}
	if options.CertFile == "" || options.KeyFile == "" {
		return nil, diagnostics, errors.New("both TLS certificate and key are required")
	}
	if err := requirePrivatePermissions(options.KeyFile); err != nil {
		return nil, diagnostics, err
	}
	certificate, err := tls.LoadX509KeyPair(options.CertFile, options.KeyFile)
	if err != nil {
		return nil, diagnostics, fmt.Errorf("load TLS certificate: %w", err)
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}
	if options.ClientCA != "" {
		pemBytes, err := os.ReadFile(options.ClientCA)
		if err != nil {
			return nil, diagnostics, fmt.Errorf("read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, diagnostics, errors.New("client CA contains no certificates")
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
		diagnostics.ClientCAEnabled = true
	}
	diagnostics.Enabled = true
	diagnostics.CertificateFile = options.CertFile
	diagnostics.KeyFile = options.KeyFile
	return config, diagnostics, nil
}

func ensureLocalCertificate(profileDir, serverName string) (string, string, error) {
	if profileDir == "" {
		return "", "", errors.New("profile directory is required for auto TLS")
	}
	if serverName == "" {
		serverName = "localhost"
	}
	dir := filepath.Join(profileDir, "tls")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	if err := requirePrivateDirectory(dir); err != nil {
		return "", "", err
	}
	certPath := filepath.Join(dir, "server-cert.pem")
	keyPath := filepath.Join(dir, "server-key.pem")
	if _, certErr := os.Stat(certPath); certErr == nil {
		if _, keyErr := os.Stat(keyPath); keyErr == nil {
			if err := requirePrivatePermissions(keyPath); err != nil {
				return "", "", err
			}
			if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
				return "", "", fmt.Errorf("load generated TLS certificate: %w", err)
			}
			return certPath, keyPath, nil
		}
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return "", "", err
	}
	now := time.Now().UTC()
	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "MiniSky local " + serverName},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(serverName); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{serverName}
		if serverName == "localhost" {
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", "", err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return "", "", err
	}
	if err := atomicWrite(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", "", err
	}
	if err := atomicWrite(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

func requirePrivatePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect private key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("private key %s must have permissions 0600 or stricter", path)
	}
	return nil
}

func requirePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect private directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("private directory must not be a symlink")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("private directory %s must have permissions 0700 or stricter", path)
	}
	return nil
}

func normalizedScopes(scopes []string) []string {
	result := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		result = append(result, scope)
	}
	return result
}

func hasScope(scopes []string, required string) bool {
	for _, scope := range scopes {
		if scope == required || scope == "https://www.googleapis.com/auth/cloud-platform" {
			return true
		}
	}
	return false
}

func writeExclusive(path string, payload []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		_ = os.Remove(path)
		return err
	}
	return file.Close()
}

func atomicWrite(path string, payload []byte, mode os.FileMode) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".minisky-security-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(mode); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
