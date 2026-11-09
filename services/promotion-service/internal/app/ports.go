package app

import (
	"context"
	"time"

	"grab-mini/promotion-service/internal/domain"
)

// PromotionReader reads what validating a code needs.
type PromotionReader interface {
	// Find returns the promotion with the code, or domain.ErrNotFound.
	Find(ctx context.Context, code string) (domain.Promotion, error)
	// Usage counts the redemptions against the code: by anyone, and by the rider.
	Usage(ctx context.Context, code, riderID string) (domain.Usage, error)
}

// PromotionWriter is the admin side of the store.
type PromotionWriter interface {
	// Create stores a promotion; domain.ErrCodeExists on a unique violation.
	Create(ctx context.Context, p domain.Promotion, createdBy string) error
	// Disable sets disabled_at to at unless it is already set; found is false
	// when no promotion has the code.
	Disable(ctx context.Context, code string, at time.Time) (found bool, err error)
}

// PromotionViews reads promotions with their usage, for admins.
type PromotionViews interface {
	// View returns one promotion, or domain.ErrNotFound.
	View(ctx context.Context, code string) (domain.Summary, error)
	// List returns every promotion, newest first.
	List(ctx context.Context) ([]domain.Summary, error)
}

// TokenIssuer signs the token Order Service checks when the rider books.
type TokenIssuer interface {
	Issue(code, riderID string, price int64, vehicle string, discount int64, now time.Time) (token string, expiresAt time.Time)
}

// Metrics are the counters the use cases move.
type Metrics interface {
	Validation(result string)
	Redemption(status, reason string)
	Released(n int64)
	Duplicate()
}
