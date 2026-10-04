package kafkaconn

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlaintextMeansNoSecurity(t *testing.T) {
	for _, p := range []string{"", "plaintext", "PLAINTEXT"} {
		s, err := New(Options{Protocol: p})
		if err != nil || s != nil {
			t.Errorf("%q: %v, %v; want nil, nil", p, s, err)
		}
	}
	// A nil Security still gives usable (plain) settings.
	var s *Security
	if d := s.Dialer(); d.TLS != nil || d.SASLMechanism != nil {
		t.Error("a nil Security's dialer has security on")
	}
	if tr := s.Transport(); tr.TLS != nil || tr.SASL != nil {
		t.Error("a nil Security's transport has security on")
	}
}

func TestSASLSSLTurnsOnBoth(t *testing.T) {
	s, err := New(Options{Protocol: "SASL_SSL", Username: "svc", Password: "pw"})
	if err != nil {
		t.Fatal(err)
	}
	if s.TLS == nil || s.SASL == nil {
		t.Fatalf("TLS=%v SASL=%v, want both", s.TLS != nil, s.SASL != nil)
	}
	if s.TLS.MinVersion < 0x0303 {
		t.Error("TLS below 1.2 is allowed")
	}
	if s.SASL.Name() != "SCRAM-SHA-512" {
		t.Errorf("default mechanism %q", s.SASL.Name())
	}
	if d := s.Dialer(); d.TLS == nil || d.SASLMechanism == nil {
		t.Error("the dialer lost the security settings")
	}
	if tr := s.Transport(); tr.TLS == nil || tr.SASL == nil {
		t.Error("the transport lost the security settings")
	}
}

func TestMechanisms(t *testing.T) {
	for name, want := range map[string]string{"SCRAM-SHA-256": "SCRAM-SHA-256", "scram-sha-512": "SCRAM-SHA-512", "PLAIN": "PLAIN"} {
		s, err := New(Options{Protocol: "SASL_PLAINTEXT", Mechanism: name, Username: "u", Password: "p"})
		if err != nil || s.SASL.Name() != want || s.TLS != nil {
			t.Errorf("%s: %v, %v", name, s, err)
		}
	}
	if _, err := New(Options{Protocol: "SASL_SSL", Mechanism: "GSSAPI", Username: "u", Password: "p"}); err == nil {
		t.Error("accepted an unsupported mechanism")
	}
}

func TestMisconfigurationIsAnErrorNotSilentlyPlaintext(t *testing.T) {
	cases := map[string]Options{
		"unknown protocol":      {Protocol: "TLS"},
		"SASL without user":     {Protocol: "SASL_SSL", Password: "p"},
		"SASL without password": {Protocol: "SASL_SSL", Username: "u"},
		"missing CA file":       {Protocol: "SSL", CAFile: filepath.Join(t.TempDir(), "nope.pem")},
	}
	for name, o := range cases {
		if _, err := New(o); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	bad := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(bad, []byte("not a certificate"), 0o600)
	if _, err := New(Options{Protocol: "SSL", CAFile: bad}); err == nil || !strings.Contains(err.Error(), "PEM") {
		t.Errorf("a CA file with no certificate: %v", err)
	}
}

func TestFromEnvReadsThePasswordFromAFile(t *testing.T) {
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("from-file\n"), 0o600)
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
	t.Setenv("KAFKA_USERNAME", "svc")
	t.Setenv("KAFKA_PASSWORD_FILE", pw)

	s, err := FromEnv()
	if err != nil || s == nil || s.SASL == nil {
		t.Fatalf("FromEnv = %v, %v", s, err)
	}
}

func TestProductionRefusesPlaintext(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "")
	if _, err := FromEnv(); err == nil {
		t.Error("production accepted an unauthenticated plaintext connection")
	}
	t.Setenv("KAFKA_ALLOW_PLAINTEXT", "true")
	if s, err := FromEnv(); err != nil || s != nil {
		t.Errorf("with the override: %v, %v", s, err)
	}
}
