// Package arch holds the architecture test: it fails when a package under
// internal/ imports something its layer may not (docs/adr/0014). The rule and
// the checker live in services/pkg/archtest, shared with the other migrated
// services; this file only says which service it is.
//
//	domain      the standard library only (not net/http, database/sql, os ...)
//	app         domain, the standard library (same exclusions), github.com/google/uuid,
//	            grab-mini/pkg/surge and grab-mini/pkg/promo; never an adapter,
//	            an I/O library, or any other services/pkg/*
//	adapters/X  anything except another adapter
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
	rules := archtest.Default(module)
	// The quote and promo decision logic lives in app and uses *surge.Signer,
	// surge.Quote and *promo.Signer directly. Both packages are pure (HMAC,
	// encoding and maths on crypto/*, encoding/*, math and strings; only main
	// calls their SignerFromEnv), so they are allowed here and nothing else
	// from services/pkg is (plan decision 1).
	rules.AppAllowed = append(rules.AppAllowed, "grab-mini/pkg/surge", "grab-mini/pkg/promo")
	rules.Test(t, ".")
}
