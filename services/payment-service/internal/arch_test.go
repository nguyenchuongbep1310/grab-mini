// Package arch holds the architecture test: it fails when a package under
// internal/ imports something its layer may not (docs/adr/0014).
//
//	domain      the standard library only (not net/http, database/sql, os ...)
//	app         domain, the standard library (same exclusions), github.com/google/uuid;
//	            never an adapter, an I/O library, or services/pkg/*
//	adapters/X  anything except another adapter
//
// Only imports are checked, and only in non-test files: the integration tests
// wire the adapters together on purpose.
package arch

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// stdlibBanned are standard-library packages that mean I/O or a transport; the
// domain and the app reach them only through ports.
var stdlibBanned = map[string]bool{
	"net": true, "net/http": true, "net/rpc": true, "database/sql": true,
	"os": true, "os/exec": true, "os/signal": true,
}

// appThirdParty are the pure helper libraries the app may use.
var appThirdParty = map[string]bool{"github.com/google/uuid": true}

// violation says why a file in layer may not import path, or "" when it may.
// module is this service's module path; sibling is the adapter the file belongs
// to when layer is "adapters".
func violation(module, layer, adapter, path string) string {
	root := strings.SplitN(module, "/", 2)[0] // "grab-mini": the workspace's modules share it
	isStdlib := !strings.Contains(strings.SplitN(path, "/", 2)[0], ".") && !strings.HasPrefix(path, root+"/")
	switch layer {
	case "domain", "app":
		switch {
		case isStdlib:
			if stdlibBanned[path] {
				return "standard-library I/O package " + path + " (reach it through a port)"
			}
			return ""
		case strings.HasPrefix(path, module+"/internal/domain"):
			return ""
		case layer == "app" && appThirdParty[path]:
			return ""
		case strings.HasPrefix(path, module+"/internal/adapters"):
			return "an adapter (dependencies point inward: adapters -> app -> domain)"
		case strings.HasPrefix(path, module+"/internal/app"):
			return "the app layer (the domain is the innermost layer)"
		case strings.HasPrefix(path, root+"/") && !strings.HasPrefix(path, module):
			return "services/pkg/* (shared infrastructure; map the wire type in an adapter)"
		}
		return "third-party library " + path + " (I/O libraries belong in an adapter)"
	case "adapters":
		if strings.HasPrefix(path, module+"/internal/adapters/") &&
			!strings.HasPrefix(path, module+"/internal/adapters/"+adapter) {
			return "another adapter (only main wires adapters together)"
		}
	}
	return ""
}

// layerOf maps a path relative to internal/ to its layer and adapter name.
func layerOf(rel string) (layer, adapter string) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	switch parts[0] {
	case "domain", "app":
		return parts[0], ""
	case "adapters":
		if len(parts) > 2 {
			return "adapters", parts[1]
		}
	}
	return "", ""
}

func moduleOf(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if m, ok := strings.CutPrefix(line, "module "); ok {
			return strings.TrimSpace(m)
		}
	}
	t.Fatal("no module line in go.mod")
	return ""
}

func TestImportsFollowTheDependencyRule(t *testing.T) {
	module := moduleOf(t)
	var found []string
	checked := 0
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		layer, adapter := layerOf(path)
		if layer == "" {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		checked++
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if why := violation(module, layer, adapter, p); why != "" {
				found = append(found, "internal/"+filepath.ToSlash(path)+" imports "+p+": "+why)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 10 {
		t.Fatalf("only %d files checked: is the test looking in the right place?", checked)
	}
	sort.Strings(found)
	for _, v := range found {
		t.Error(v)
	}
}

// The rule itself, on made-up imports, so a refactor of violation cannot
// silently turn the check into a no-op.
func TestTheRuleRefusesWhatItShould(t *testing.T) {
	const m = "grab-mini/payment-service"
	cases := []struct {
		layer, adapter, path string
		bad                  bool
	}{
		{"domain", "", "fmt", false},
		{"domain", "", "math/rand", false},
		{"domain", "", m + "/internal/domain", false},
		{"domain", "", "net/http", true},
		{"domain", "", "database/sql", true},
		{"domain", "", "os", true},
		{"domain", "", "github.com/google/uuid", true},
		{"domain", "", "github.com/jackc/pgx/v5", true},
		{"domain", "", "github.com/prometheus/client_golang/prometheus", true},
		{"domain", "", "grab-mini/pkg/events", true},
		{"domain", "", m + "/internal/app", true},
		{"app", "", "github.com/google/uuid", false},
		{"app", "", m + "/internal/domain", false},
		{"app", "", "log", false},
		{"app", "", "net/http", true},
		{"app", "", "github.com/segmentio/kafka-go", true},
		{"app", "", "github.com/redis/go-redis/v9", true},
		{"app", "", "github.com/jackc/pgx/v5/pgxpool", true},
		{"app", "", "github.com/prometheus/client_golang/prometheus/promauto", true},
		{"app", "", "grab-mini/pkg/outbox", true},
		{"app", "", m + "/internal/adapters/postgres", true},
		{"adapters", "postgres", "github.com/jackc/pgx/v5", false},
		{"adapters", "postgres", "grab-mini/pkg/outbox", false},
		{"adapters", "postgres", m + "/internal/app", false},
		{"adapters", "postgres", m + "/internal/adapters/postgres", false},
		{"adapters", "postgres", m + "/internal/adapters/kafka", true},
	}
	for _, c := range cases {
		got := violation(m, c.layer, c.adapter, c.path)
		if (got != "") != c.bad {
			t.Errorf("%s %s imports %s: violation %q, want violation=%v", c.layer, c.adapter, c.path, got, c.bad)
		}
	}
}
