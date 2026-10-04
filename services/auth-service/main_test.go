package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
)

const testSecret = "0123456789abcdef0123456789abcdef"

func post(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	issuer, err := auth.NewIssuer(testSecret, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	newMux(issuer).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(body)))
	return rec
}

func TestIssuesATokenTheServicesAccept(t *testing.T) {
	rec := post(t, `{"role":"driver","id":"driver-3"}`)
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d, cache %q", rec.Code, rec.Header().Get("Cache-Control"))
	}
	var resp tokenResponse
	if err := jsonDecode(rec, &resp); err != nil {
		t.Fatal(err)
	}
	v, _ := auth.NewVerifier(testSecret)
	if id, err := v.Verify(resp.Token); err != nil || id.Subject != "driver-3" || id.Role != auth.Driver {
		t.Errorf("Verify = %+v, %v", id, err)
	}
}

func TestRejectsBadRequests(t *testing.T) {
	for name, body := range map[string]string{
		"not JSON":     `nope`,
		"no role":      `{"id":"rider-1"}`,
		"unknown role": `{"role":"admin","id":"x"}`,
		"no id":        `{"role":"rider"}`,
		"odd id":       `{"role":"rider","id":"../etc"}`,
		"id too long":  `{"role":"rider","id":"` + strings.Repeat("a", 65) + `"}`,
		"huge body":    `{"role":"rider","id":"x","pad":"` + strings.Repeat("a", 2000) + `"}`,
	} {
		if rec := post(t, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, rec.Code)
		}
	}
}

func jsonDecode(rec *httptest.ResponseRecorder, v any) error {
	return json.NewDecoder(rec.Body).Decode(v)
}
