package metadata

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
)

func TestMetadataTokenUsesLocalIssuer(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), func() time.Time { return now })
	api := &API{meta: defaultMeta, issuer: issuer}
	response := httptest.NewRecorder()
	api.handleToken(response, httptest.NewRequest(http.MethodGet, "/computeMetadata/v1/instance/service-accounts/default/token", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	claims, err := issuer.Verify(body.AccessToken, localsecurity.VerifyOptions{
		Audience: "minisky-gateway", RequiredScope: "https://www.googleapis.com/auth/cloud-platform",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "serviceAccount:"+defaultMeta.ServiceAccount {
		t.Fatalf("subject=%q", claims.Subject)
	}
}
