// Package arch holds the architecture test: it fails when a package under
// internal/ imports something its layer may not (docs/adr/0014). The rule and
// the checker live in services/pkg/archtest, shared with the other migrated
// services; this file only says which service it is.
//
//	domain      the standard library only (not net/http, database/sql, os ...)
//	app         domain, the standard library (same exclusions), github.com/google/uuid;
//	            never an adapter, an I/O library, or anything under services/pkg/*
//	            (the promo signer sits behind app.TokenIssuer: pkg/promo imports
//	            pkg/secret, which imports os)
//	adapters/X  anything except another adapter
//
// The check looks at direct imports only: a package an allowed package imports
// in turn (uuid imports net and os) is not seen.
package arch

import (
	"testing"

	"grab-mini/pkg/archtest"
)

func TestImportsFollowTheDependencyRule(t *testing.T) {
	module, err := archtest.ModulePath("../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	archtest.Default(module).Test(t, ".")
}
