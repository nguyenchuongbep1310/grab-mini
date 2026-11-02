// Package archtest checks the dependency rule of docs/adr/0014 from a service's
// own `go test`: it parses the imports of every non-test .go file under the
// service's internal/ directory and reports the ones its layer may not use.
//
//	domain      the standard library only (not net/http, database/sql, os ...)
//	app         domain, the standard library (same exclusions), and an explicit
//	            allow-list of pure libraries; never an adapter, an I/O library,
//	            or the shared kit (services/pkg/*)
//	adapters/X  anything except another adapter
//
// Only imports are checked, and only in non-test files: the integration tests
// wire the adapters together on purpose. This is a dev-time helper: it carries
// no business rule and a service imports it from _test.go files only.
//
// A service's internal/arch_test.go is then a few lines:
//
//	func TestImportsFollowTheDependencyRule(t *testing.T) {
//		module, err := archtest.ModulePath("../go.mod")
//		if err != nil { t.Fatal(err) }
//		archtest.Default(module).Test(t, ".")
//	}
package archtest

import (
	"fmt"
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

// The layers a file can belong to.
const (
	Domain   = "domain"
	App      = "app"
	Adapters = "adapters"
)

// Rules is the dependency rule for one service. Build it with Default and
// change the fields a service really needs to differ in.
type Rules struct {
	// Module is the service's module path, for example "grab-mini/payment-service".
	Module string
	// SharedPrefix is the first path element the workspace's modules share
	// ("grab-mini"). An import under it that is not under Module is the shared
	// kit (services/pkg/*), which domain and app must not reach.
	SharedPrefix string
	// The layer directories, relative to internal/.
	DomainDir, AppDir, AdaptersDir string
	// StdlibBanned are standard-library packages that mean I/O or a transport;
	// the domain and the app reach them only through ports.
	StdlibBanned []string
	// AppAllowed are the pure helper libraries the app may import.
	AppAllowed []string
	// DomainAllowed are the third-party libraries the domain may import (none by default).
	DomainAllowed []string
	// MinFiles fails the test when fewer layer files were checked, so a test
	// run from the wrong directory cannot pass by checking nothing.
	MinFiles int
}

// Default is the rule of ADR 0014 for the service whose module path is module.
func Default(module string) Rules {
	return Rules{
		Module:       module,
		SharedPrefix: strings.SplitN(module, "/", 2)[0],
		DomainDir:    "domain",
		AppDir:       "app",
		AdaptersDir:  "adapters",
		StdlibBanned: []string{
			"net", "net/http", "net/rpc", "database/sql",
			"os", "os/exec", "os/signal",
		},
		AppAllowed: []string{"github.com/google/uuid"},
		MinFiles:   10,
	}
}

// ModulePath reads the module path from a go.mod file.
func ModulePath(goMod string) (string, error) {
	b, err := os.ReadFile(goMod)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if m, ok := strings.CutPrefix(line, "module "); ok {
			return strings.TrimSpace(m), nil
		}
	}
	return "", fmt.Errorf("archtest: no module line in %s", goMod)
}

func in(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// Violation says why a file in layer may not import path, or "" when it may.
// adapter is the adapter package the file belongs to when layer is Adapters.
func (r Rules) Violation(layer, adapter, path string) string {
	isStdlib := !strings.Contains(strings.SplitN(path, "/", 2)[0], ".") && !strings.HasPrefix(path, r.SharedPrefix+"/")
	internal := r.Module + "/internal/"
	switch layer {
	case Domain, App:
		switch {
		case isStdlib:
			if in(r.StdlibBanned, path) {
				return "standard-library I/O package " + path + " (reach it through a port)"
			}
			return ""
		case strings.HasPrefix(path, internal+r.DomainDir):
			return ""
		case layer == App && in(r.AppAllowed, path), layer == Domain && in(r.DomainAllowed, path):
			return ""
		case strings.HasPrefix(path, internal+r.AdaptersDir):
			return "an adapter (dependencies point inward: adapters -> app -> domain)"
		case strings.HasPrefix(path, internal+r.AppDir):
			return "the app layer (the domain is the innermost layer)"
		case strings.HasPrefix(path, r.SharedPrefix+"/") && !strings.HasPrefix(path, r.Module):
			return "services/pkg/* (shared infrastructure; map the wire type in an adapter)"
		}
		return "third-party library " + path + " (I/O libraries belong in an adapter)"
	case Adapters:
		if strings.HasPrefix(path, internal+r.AdaptersDir+"/") &&
			!strings.HasPrefix(path, internal+r.AdaptersDir+"/"+adapter) {
			return "another adapter (only main wires adapters together)"
		}
	}
	return ""
}

// LayerOf maps a path relative to internal/ to its layer and adapter name;
// layer is "" for files that are not in a layer (arch_test.go, integration/).
func (r Rules) LayerOf(rel string) (layer, adapter string) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	switch parts[0] {
	case r.DomainDir:
		return Domain, ""
	case r.AppDir:
		return App, ""
	case r.AdaptersDir:
		if len(parts) > 2 {
			return Adapters, parts[1]
		}
	}
	return "", ""
}

// Scan checks every non-test .go file below dir (the service's internal
// directory) and returns the violations, sorted, as
// "internal/<path> imports X: <why>", with the number of layer files checked.
func (r Rules) Scan(dir string) (violations []string, checked int, err error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, 0, err
	}
	base := filepath.Base(abs)
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		layer, adapter := r.LayerOf(rel)
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
			if why := r.Violation(layer, adapter, p); why != "" {
				violations = append(violations, base+"/"+filepath.ToSlash(rel)+" imports "+p+": "+why)
			}
		}
		return nil
	})
	sort.Strings(violations)
	return violations, checked, err
}

// Test runs Scan on dir and fails t with one error per violation.
func (r Rules) Test(t testing.TB, dir string) {
	t.Helper()
	found, checked, err := r.Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	if checked < r.MinFiles {
		t.Fatalf("only %d files checked: is the test looking in the right place?", checked)
	}
	for _, v := range found {
		t.Error(v)
	}
}
