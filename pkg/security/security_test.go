package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIssuerVerifiesExpiryAudienceAndScope(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	issuer := NewIssuer([]byte("01234567890123456789012345678901"), func() time.Time { return now })
	token, claims, err := issuer.Issue(TokenRequest{
		Subject:  "serviceAccount:worker@example.iam.gserviceaccount.com",
		Audience: "minisky-gateway",
		Scopes:   []string{"storage.objects.get"},
		Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || claims.ExpiresAt.Sub(now) != time.Minute {
		t.Fatalf("unexpected token result: %#v", claims)
	}
	if _, err := issuer.Verify(token, VerifyOptions{
		Audience:      "minisky-gateway",
		RequiredScope: "storage.objects.get",
	}); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := issuer.Verify(token, VerifyOptions{Audience: "other"}); err != ErrAudience {
		t.Fatalf("audience error = %v", err)
	}
	if _, err := issuer.Verify(token, VerifyOptions{RequiredScope: "storage.objects.delete"}); err != ErrScope {
		t.Fatalf("scope error = %v", err)
	}
	issuer.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err := issuer.Verify(token, VerifyOptions{}); err != ErrExpired {
		t.Fatalf("expiry error = %v", err)
	}
}

func TestPrepareTLSAutoGeneratesSecureProfileMaterial(t *testing.T) {
	root := t.TempDir()
	config, diagnostics, err := PrepareTLS(TLSOptions{
		Mode:       TLSAuto,
		ProfileDir: root,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	if config == nil || diagnostics.CertificateFile == "" || diagnostics.KeyFile == "" {
		t.Fatalf("missing TLS output: %#v", diagnostics)
	}
	info, err := os.Stat(diagnostics.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("key permissions = %o", info.Mode().Perm())
	}
	block, _ := pem.Decode(mustRead(t, diagnostics.CertificateFile))
	if block == nil {
		t.Fatal("certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := cert.VerifyHostname("localhost"); err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(diagnostics.KeyFile) != filepath.Join(root, "tls") {
		t.Fatalf("key path = %s", diagnostics.KeyFile)
	}
}

func TestPrepareTLSRejectsPartialAndInsecureConfiguration(t *testing.T) {
	if _, _, err := PrepareTLS(TLSOptions{Mode: TLSFiles, CertFile: "cert.pem"}); err == nil {
		t.Fatal("expected partial configuration error")
	}

	root := t.TempDir()
	key := filepath.Join(root, "key.pem")
	if err := os.WriteFile(key, []byte("not a key"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := PrepareTLS(TLSOptions{Mode: TLSFiles, CertFile: key, KeyFile: key}); err == nil {
		t.Fatal("expected insecure key error")
	}

	profile := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(profile, "tls")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := PrepareTLS(TLSOptions{Mode: TLSAuto, ProfileDir: profile}); err == nil {
		t.Fatal("expected symlinked TLS directory rejection")
	}
}

func TestMTLSHandshakeRequiresClientCertificate(t *testing.T) {
	root := t.TempDir()
	_, generated, err := PrepareTLS(TLSOptions{Mode: TLSAuto, ProfileDir: root, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	caPEM, clientCert := makeClientCertificate(t)
	caPath := filepath.Join(root, "client-ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	serverTLS, diagnostics, err := PrepareTLS(TLSOptions{
		Mode: TLSFiles, CertFile: generated.CertificateFile, KeyFile: generated.KeyFile, ClientCA: caPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !diagnostics.ClientCAEnabled {
		t.Fatal("mTLS diagnostics were not enabled")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()

	if _, err := server.Client().Get(server.URL); err == nil {
		t.Fatal("handshake succeeded without a client certificate")
	}
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.Certificates = []tls.Certificate{clientCert}
	client := &http.Client{Transport: transport}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("mTLS handshake: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func makeClientCertificate(t *testing.T) ([]byte, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "MiniSky test client CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "MiniSky test client"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKeyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), clientCert
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
