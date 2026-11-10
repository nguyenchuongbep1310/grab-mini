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

// UnitOfWork runs fn in one transaction: committed when fn returns nil (Do
// then returns the commit's error), rolled back, with everything fn wrote,
// when fn returns an error, which Do returns as it is.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(Tx) error) error
}

// Tx is what applying an event does inside its transaction.
type Tx interface {
	// MarkEventProcessed records eventID as applied; first is false when it
	// already was. It does not check eventID's shape.
	MarkEventProcessed(ctx context.Context, eventID string) (first bool, err error)
	// LockOrder serialises a redemption and a release of one order until the
	// transaction ends.
	LockOrder(ctx context.Context, orderID string) error
	// FindPromotionForUpdate returns the promotion with the code, its row
	// locked, or domain.ErrNotFound.
	FindPromotionForUpdate(ctx context.Context, code string) (domain.Promotion, error)
	// OrderWasCancelled says whether a release named the order before any
	// redemption of it was recorded.
	OrderWasCancelled(ctx context.Context, orderID string) (bool, error)
	// Usage counts the live redemptions against the code: by anyone, and by the rider.
	Usage(ctx context.Context, code, riderID string) (domain.Usage, error)
	// InsertRedemption records r unless (code, order) already is; inserted is
	// false then.
	InsertRedemption(ctx context.Context, r NewRedemption) (inserted bool, err error)
	// QueueRedeemed adds a promotion.redeemed outbox row keyed by the order.
	QueueRedeemed(ctx context.Context, orderID string, payload []byte) error
	// ReleaseRedemptions moves the order's REDEEMED and OVER_LIMIT rows to
	// RELEASED, stamped with the database clock, and says how many moved.
	ReleaseRedemptions(ctx context.Context, orderID string) (released int64, err error)
	// RememberCancelled records that the order ended before any redemption of
	// it was seen.
	RememberCancelled(ctx context.Context, orderID string) error
}

// NewRedemption is a redemptions row. An empty OverLimitReason is stored as
// NULL; ReleasedAt is set only for an order already cancelled when recorded.
type NewRedemption struct {
	OrderID, Code, RiderID string
	Discount               int64
	StoredStatus           string
	OverLimitReason        string
	SourceEventID          string
	ReleasedAt             *time.Time
}

// RedeemedEvent is what promotion.redeemed announces.
type RedeemedEvent struct {
	EventID                string
	OccurredAt             time.Time
	OrderID, RiderID, Code string
	Discount               int64
	Status, Currency       string
}

// Encoder serialises the events this service publishes.
type Encoder interface {
	Redeemed(ctx context.Context, evt RedeemedEvent) ([]byte, error)
}

// Waker nudges the outbox relay to publish what was just committed.
type Waker interface {
	Wake()
}
