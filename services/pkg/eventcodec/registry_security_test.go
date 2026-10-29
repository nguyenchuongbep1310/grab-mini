package eventcodec

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"grab-mini/pkg/events"
)

// clearRegistryEnv makes the tests independent of the developer's shell.
func clearRegistryEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"SCHEMA_REGISTRY_USER", "SCHEMA_REGISTRY_USER_FILE", "SCHEMA_REGISTRY_PASSWORD",
		"SCHEMA_REGISTRY_PASSWORD_FILE", "SCHEMA_REGISTRY_CA_FILE", "APP_ENV"} {
		t.Setenv(k, "")
	}
}

func TestAnOversizedRegistryResponseIsRefused(t *testing.T) {
	clearRegistryEnv(t)
	for name, status := range map[string]int{"success body": http.StatusOK, "error body": http.StatusInternalServerError} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			chunk := []byte(strings.Repeat("x", 64<<10))
			for i := 0; i < (MaxResponseBytes/len(chunk))+4; i++ {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		}))
		c := New(srv.URL)
		_, err := c.Encode(context.Background(), events.TopicOrderCreated, sampleOrder)
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), "larger than") {
			t.Errorf("%s: err = %v, want a size refusal", name, err)
		}
	}
}

func TestBasicAuthIsSentFromEnvOrOption(t *testing.T) {
	clearRegistryEnv(t)
	var got []string
	reg := &fakeRegistry{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if ok {
			got = append(got, u+":"+p)
		} else {
			got = append(got, "")
		}
		reg.ServeHTTP(w, r)
	}))
	defer srv.Close()

	// No credentials configured (local development): no header at all.
	if _, err := New(srv.URL).Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || got[0] != "" {
		t.Fatalf("unauthenticated registry got credentials: %q", got)
	}

	// From the environment, password through a mounted file.
	pw := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(pw, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCHEMA_REGISTRY_USER", "svc")
	t.Setenv("SCHEMA_REGISTRY_PASSWORD_FILE", pw)
	got = nil
	if _, err := New(srv.URL).Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || got[0] != "svc:s3cret" {
		t.Errorf("env credentials: got %q", got)
	}

	// An explicit option wins over the environment.
	got = nil
	if _, err := New(srv.URL, WithBasicAuth("other", "pw2")).Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || got[0] != "other:pw2" {
		t.Errorf("option credentials: got %q", got)
	}
}

func TestBadRegistrySettingsAreReportedNotIgnored(t *testing.T) {
	clearRegistryEnv(t)
	srv := httptest.NewServer(&fakeRegistry{})
	defer srv.Close()
	encode := func() error {
		_, err := New(srv.URL).Encode(context.Background(), events.TopicOrderCreated, sampleOrder)
		return err
	}

	t.Setenv("SCHEMA_REGISTRY_USER", "svc")
	if err := encode(); err == nil || !strings.Contains(err.Error(), "together") {
		t.Errorf("user without password: %v", err)
	}

	t.Setenv("SCHEMA_REGISTRY_PASSWORD", "pw")
	t.Setenv("APP_ENV", "production")
	if err := encode(); err == nil || !strings.Contains(err.Error(), "plain http") {
		t.Errorf("credentials over http in production: %v", err)
	}

	t.Setenv("APP_ENV", "")
	t.Setenv("SCHEMA_REGISTRY_CA_FILE", filepath.Join(t.TempDir(), "missing.pem"))
	if err := encode(); err == nil || !strings.Contains(err.Error(), "SCHEMA_REGISTRY_CA_FILE") {
		t.Errorf("missing CA file: %v", err)
	}
}

func TestHTTPSRegistryWithAPrivateCA(t *testing.T) {
	clearRegistryEnv(t)
	srv := httptest.NewTLSServer(&fakeRegistry{})
	defer srv.Close()

	// Without the CA the certificate is rejected.
	if _, err := New(srv.URL).Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err == nil {
		t.Fatal("an https registry with an untrusted certificate was accepted")
	}

	ca := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pemOf(srv.Certificate().Raw)
	if err := os.WriteFile(ca, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SCHEMA_REGISTRY_CA_FILE", ca)
	if _, err := New(srv.URL).Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err != nil {
		t.Errorf("with the CA: %v", err)
	}
}
