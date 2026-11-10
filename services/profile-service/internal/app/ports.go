package app

import (
	"context"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// RiderStore keeps rider profiles.
type RiderStore interface {
	// Rider reports false (and no error) when the rider has no row.
	Rider(ctx context.Context, id string) (domain.Rider, bool, error)
	SaveRider(ctx context.Context, r domain.Rider) error
}

// DriverReader reads a driver profile outside any transaction.
type DriverReader interface {
	// Driver returns domain.ErrNoDriver when there is no row.
	Driver(ctx context.Context, id string) (domain.Driver, error)
}

// ApplicationQueue lists drivers by status, oldest status change first.
type ApplicationQueue interface {
	// ByStatus returns up to limit drivers after the cursor (nil: from the start).
	ByStatus(ctx context.Context, status string, limit int, after *domain.Cursor) ([]domain.Driver, error)
}

// ResyncSource lists the ids of drivers whose status is a result (APPROVED, REJECTED, SUSPENDED).
type ResyncSource interface {
	ResyncableIDs(ctx context.Context, after string, limit int) ([]string, error)
}

// ResyncLocker lets one resync run at a time across instances.
type ResyncLocker interface {
	// TryLock reports acquired=false when another run holds the lock. unlock must be called
	// once when acquired.
	TryLock(ctx context.Context) (unlock func(), acquired bool, err error)
}

// UnitOfWork runs fn in one transaction: commit when fn returns nil, roll back otherwise.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(Tx) error) error
}

// Tx is the narrow set of statements the driver use cases run inside a transaction.
type Tx interface {
	// LockOrCreateDriver creates the (empty) profile if there is none and locks it.
	LockOrCreateDriver(ctx context.Context, id string, now time.Time) (domain.Driver, error)
	// LockDriver locks an existing profile; domain.ErrNoDriver when there is none.
	LockDriver(ctx context.Context, id string) (domain.Driver, error)
	// UpdateProfile writes the editable fields and the status columns.
	UpdateProfile(ctx context.Context, d domain.Driver) error
	// MarkPending writes an application: PENDING, no reason, status and update time now.
	MarkPending(ctx context.Context, id string, now time.Time) error
	// RecordReview writes a decision: reviewed_at = at, status_at = updated_at = d.StatusAt.
	RecordReview(ctx context.Context, d domain.Driver, at time.Time) error
	// QueueReview adds driver.application.reviewed to the outbox in the transaction.
	QueueReview(ctx context.Context, driverID string, payload []byte) error
}

// ReviewedEvent is the review result as the use case sees it; the adapter owns the wire form.
type ReviewedEvent struct {
	EventID     string
	OccurredAt  time.Time
	DriverID    string
	Status      string
	VehicleType domain.VehicleType
	Reason      string
	ReviewedBy  string
}

// Encoder turns a review result into the bytes to queue.
type Encoder interface {
	Encode(ctx context.Context, e ReviewedEvent) ([]byte, error)
}

// Waker nudges the outbox relay after a commit.
type Waker interface{ Wake() }

// Metrics are the counters the use cases touch.
type Metrics interface {
	Updated(kind string)
	Application(result string)
	Review(decision string)
	PublicLookup(result string)
	ResyncEvents(status string, n int)
	ResyncRun(result string)
}
