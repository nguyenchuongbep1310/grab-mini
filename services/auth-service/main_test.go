package main

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
)

var testPub, testPriv, _ = ed25519.GenerateKey(nil)

func post(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	issuer, err := auth.NewIssuer(testPriv, time.Hour)
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
	v := auth.NewVerifier(auth.NewKeySet(testPub))
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

func TestPublishesTheVerificationKeysAsJWKS(t *testing.T) {
	issuer, _ := auth.NewIssuer(testPriv, time.Hour)
	oldPub, _, _ := ed25519.GenerateKey(nil)
	rec := httptest.NewRecorder()
	newMux(issuer, oldPub).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/jwks.json", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "public") || !strings.Contains(cc, "max-age=") {
		t.Errorf("Cache-Control = %q, want public with a max-age", cc)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "json") {
		t.Errorf("Content-Type = %q", ct)
	}
	keys, err := auth.ParseJWKS(rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || !keys[issuer.KeyID()].Equal(testPub) || !keys[auth.KeyID(oldPub)].Equal(oldPub) {
		t.Errorf("published keys = %d, want the signing key and the retired one", len(keys))
	}
	// The private key must never appear in what is published.
	if strings.Contains(rec.Body.String(), `"d"`) {
		t.Error("the JWKS has a private member")
	}
}

func TestATokenVerifiesAgainstThePublishedJWKS(t *testing.T) {
	issuer, _ := auth.NewIssuer(testPriv, time.Hour)
	srv := httptest.NewServer(newMux(issuer))
	defer srv.Close()
	tok, _, _ := issuer.Issue(auth.Rider, "rider-9")
	v := auth.NewVerifier(auth.NewJWKSClient(srv.URL + "/.well-known/jwks.json"))
	if id, err := v.Verify(tok); err != nil || id.Subject != "rider-9" {
		t.Errorf("Verify via JWKS = %+v, %v", id, err)
	}
}
