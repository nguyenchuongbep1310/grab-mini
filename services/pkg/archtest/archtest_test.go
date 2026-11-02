package archtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The rule itself, on made-up imports, so a refactor of Violation cannot
// silently turn the check into a no-op.
func TestTheRuleRefusesWhatItShould(t *testing.T) {
	const m = "grab-mini/payment-service"
	cases := []struct {
		layer, adapter, path string
		bad                  bool
	}{
		{Domain, "", "fmt", false},
		{Domain, "", "math/rand", false},
		{Domain, "", m + "/internal/domain", false},
		{Domain, "", "net/http", true},
		{Domain, "", "database/sql", true},
		{Domain, "", "os", true},
		{Domain, "", "github.com/google/uuid", true},
		{Domain, "", "github.com/jackc/pgx/v5", true},
		{Domain, "", "github.com/prometheus/client_golang/prometheus", true},
		{Domain, "", "grab-mini/pkg/events", true},
		{Domain, "", m + "/internal/app", true},
		{App, "", "github.com/google/uuid", false},
		{App, "", m + "/internal/domain", false},
		{App, "", "log", false},
		{App, "", "net/http", true},
		{App, "", "github.com/segmentio/kafka-go", true},
		{App, "", "github.com/redis/go-redis/v9", true},
		{App, "", "github.com/jackc/pgx/v5/pgxpool", true},
		{App, "", "github.com/prometheus/client_golang/prometheus/promauto", true},
		{App, "", "grab-mini/pkg/outbox", true},
		{App, "", m + "/internal/adapters/postgres", true},
		{Adapters, "postgres", "github.com/jackc/pgx/v5", false},
		{Adapters, "postgres", "grab-mini/pkg/outbox", false},
		{Adapters, "postgres", m + "/internal/app", false},
		{Adapters, "postgres", m + "/internal/adapters/postgres", false},
		{Adapters, "postgres", m + "/internal/adapters/kafka", true},
		// another service's module is the shared kit's neighbour, not ours
		{Domain, "", "grab-mini/trip-service/internal/domain", true},
		{App, "", "grab-mini/trip-service/internal/app", true},
	}
	r := Default(m)
	for _, c := range cases {
		got := r.Violation(c.layer, c.adapter, c.path)
		if (got != "") != c.bad {
			t.Errorf("%s %s imports %s: violation %q, want violation=%v", c.layer, c.adapter, c.path, got, c.bad)
		}
	}
}

// The same table works for another service: only the module path changes.
func TestTheRuleFollowsTheModulePath(t *testing.T) {
	r := Default("grab-mini/trip-service")
	if why := r.Violation(App, "", "grab-mini/trip-service/internal/domain"); why != "" {
		t.Errorf("app -> own domain refused: %s", why)
	}
	if why := r.Violation(App, "", "grab-mini/trip-service/internal/adapters/redis"); why == "" {
		t.Error("app -> own adapter allowed")
	}
	if why := r.Violation(Adapters, "http", "grab-mini/trip-service/internal/adapters/redis"); why == "" {
		t.Error("adapter -> other adapter allowed")
	}
}

// Every knob does something.
func TestTheRuleIsConfigurable(t *testing.T) {
	r := Default("example.com/svc")
	r.DomainDir, r.AppDir, r.AdaptersDir = "core", "usecases", "edges"
	r.StdlibBanned = append(r.StdlibBanned, "log")
	r.AppAllowed = append(r.AppAllowed, "github.com/shopspring/decimal")
	r.DomainAllowed = []string{"github.com/shopspring/decimal"}
	for _, c := range []struct {
		layer, adapter, path string
		bad                  bool
	}{
		{Domain, "", "log", true},
		{Domain, "", "fmt", false},
		{Domain, "", "github.com/shopspring/decimal", false},
		{Domain, "", "github.com/google/uuid", true}, // the default allow-list was replaced for the domain only
		{App, "", "github.com/google/uuid", false},
		{App, "", "github.com/shopspring/decimal", false},
		{App, "", "example.com/svc/internal/core", false},
		{App, "", "example.com/svc/internal/domain", true}, // a third-party-looking path now
		{App, "", "example.com/svc/internal/edges/db", true},
		{Domain, "", "example.com/svc/internal/usecases", true},
		{Adapters, "db", "example.com/svc/internal/edges/queue", true},
		{Adapters, "db", "example.com/svc/internal/edges/db", false},
	} {
		if got := r.Violation(c.layer, c.adapter, c.path); (got != "") != c.bad {
			t.Errorf("%s %s imports %s: violation %q, want violation=%v", c.layer, c.adapter, c.path, got, c.bad)
		}
	}
}

func TestLayerOf(t *testing.T) {
	r := Default("grab-mini/x")
	for rel, want := range map[string][2]string{
		"domain/a.go":                {Domain, ""},
		"domain/sub/a.go":            {Domain, ""},
		"app/a.go":                   {App, ""},
		"adapters/postgres/a.go":     {Adapters, "postgres"},
		"adapters/postgres/sub/a.go": {Adapters, "postgres"},
		"adapters/a.go":              {"", ""},
		"integration/a_test.go":      {"", ""},
		"arch_test.go":               {"", ""},
	} {
		if l, a := r.LayerOf(rel); l != want[0] || a != want[1] {
			t.Errorf("LayerOf(%q) = %q, %q; want %q, %q", rel, l, a, want[0], want[1])
		}
	}
}

func write(t *testing.T, dir, rel, imports string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("package x\n\nimport (\n"+imports+")\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestScanFindsViolationsAndIgnoresTestsAndOtherDirectories(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal")
	const m = "grab-mini/svc"
	write(t, dir, "domain/ok.go", `"fmt"`)
	write(t, dir, "domain/bad.go", `"net/http"`+"\n"+`_ "grab-mini/pkg/events"`)
	write(t, dir, "domain/bad_test.go", `"net/http"`) // tests may import anything
	write(t, dir, "app/bad.go", `_ "`+m+`/internal/adapters/postgres"`)
	write(t, dir, "adapters/postgres/ok.go", `_ "github.com/jackc/pgx/v5"`+"\n"+`_ "`+m+`/internal/app"`)
	write(t, dir, "adapters/kafka/bad.go", `_ "`+m+`/internal/adapters/postgres"`)
	write(t, dir, "integration/wire.go", `_ "`+m+`/internal/adapters/postgres"`+"\n"+`_ "`+m+`/internal/adapters/kafka"`)

	r := Default(m)
	got, checked, err := r.Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	if checked != 5 {
		t.Errorf("checked = %d, want 5 (integration/ and _test files are not layers)", checked)
	}
	want := []string{
		"internal/adapters/kafka/bad.go imports " + m + "/internal/adapters/postgres: another adapter (only main wires adapters together)",
		"internal/app/bad.go imports " + m + "/internal/adapters/postgres: an adapter (dependencies point inward: adapters -> app -> domain)",
		"internal/domain/bad.go imports grab-mini/pkg/events: services/pkg/* (shared infrastructure; map the wire type in an adapter)",
		"internal/domain/bad.go imports net/http: standard-library I/O package net/http (reach it through a port)",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("violations:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestScanReportsAnUnparsableFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "internal")
	if err := os.MkdirAll(filepath.Join(dir, "domain"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "domain", "x.go"), []byte("not go"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Default("grab-mini/svc").Scan(dir); err == nil {
		t.Error("a file that does not parse was skipped")
	}
}

// Test must fail when it looks at (almost) nothing and when it finds a violation.
type recorder struct {
	testing.TB
	errs  []string
	fatal string
}

func (r *recorder) Helper()                   {}
func (r *recorder) Error(args ...any)         { r.errs = append(r.errs, args[0].(string)) }
func (r *recorder) Fatalf(f string, a ...any) { r.fatal = f; panic(r) }
func (r *recorder) Fatal(a ...any)            { r.fatal = "fatal"; panic(r) }

func run(f func()) (r *recorder) {
	defer func() { recover() }()
	f()
	return nil
}

func TestTestFailsOnAnEmptyTreeAndOnAViolation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "internal")
	write(t, dir, "domain/bad.go", `"os"`)
	r := Default("grab-mini/svc")

	rec := &recorder{}
	run(func() { r.Test(rec, dir) })
	if rec.fatal == "" || !strings.Contains(rec.fatal, "files checked") {
		t.Errorf("a tree with 1 layer file passed the MinFiles guard: fatal=%q", rec.fatal)
	}

	r.MinFiles = 1
	rec = &recorder{}
	run(func() { r.Test(rec, dir) })
	if len(rec.errs) != 1 || !strings.Contains(rec.errs[0], "domain/bad.go imports os") {
		t.Errorf("errors = %v, want the os import reported", rec.errs)
	}
}

func TestModulePath(t *testing.T) {
	p := filepath.Join(t.TempDir(), "go.mod")
	if err := os.WriteFile(p, []byte("// c\nmodule grab-mini/svc\n\ngo 1.26\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if m, err := ModulePath(p); err != nil || m != "grab-mini/svc" {
		t.Errorf("ModulePath = %q, %v", m, err)
	}
	if err := os.WriteFile(p, []byte("go 1.26\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ModulePath(p); err == nil {
		t.Error("a go.mod without a module line was accepted")
	}
	if _, err := ModulePath(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing go.mod was accepted")
	}
}
