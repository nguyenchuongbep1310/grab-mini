// Package promotoken adapts the shared promo signer to app.TokenIssuer.
package promotoken

import (
	"time"

	"grab-mini/pkg/promo"
)

// Issuer signs the promo token Order Service checks when the rider books.
type Issuer struct{ signer *promo.Signer }

// New returns an Issuer over signer.
func New(signer *promo.Signer) *Issuer { return &Issuer{signer: signer} }

// Issue signs a discount for code, bound to the rider, price and vehicle.
func (i *Issuer) Issue(code, riderID string, price int64, vehicle string, discount int64, now time.Time) (string, time.Time) {
	return i.signer.Issue(code, promo.Binding{RiderID: riderID, Price: price, Vehicle: vehicle}, discount, now)
}
