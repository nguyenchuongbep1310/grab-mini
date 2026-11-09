// Package app holds the promotion use cases. It talks to the world through the
// ports in ports.go, and imports the standard library (minus I/O packages) and
// the domain only.
package app

import "time"

// Deps are the ports a Service needs. The ones the redemption use cases need
// (unit of work, event encoder, relay) join in a later change.
type Deps struct {
	Reader  PromotionReader
	Writer  PromotionWriter
	Views   PromotionViews
	Tokens  TokenIssuer
	Metrics Metrics
	Now     func() time.Time // nil: time.Now
}

// Service runs the use cases.
type Service struct {
	d Deps
}

// New returns a Service over deps.
func New(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	return &Service{d: d}
}
