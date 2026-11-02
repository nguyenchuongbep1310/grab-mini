package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
)

// Store implements the app's read ports on a pool: TripReader, RouteReader,
// CancelledOrders and Receipts.
type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

var (
	_ app.TripReader      = (*Store)(nil)
	_ app.RouteReader     = (*Store)(nil)
	_ app.CancelledOrders = (*Store)(nil)
	_ app.Receipts        = (*Store)(nil)
)

func (s *Store) ByID(ctx context.Context, id string) (domain.Trip, error) {
	return s.query(ctx, `SELECT id, order_id, driver_id, rider_id, status, rating, cancelled_by, cancel_reason, completed_at, receipt_closed_at FROM trips WHERE id = $1`, id)
}

func (s *Store) ByOrderID(ctx context.Context, orderID string) (domain.Trip, error) {
	return s.query(ctx, `SELECT id, order_id, driver_id, rider_id, status, rating, cancelled_by, cancel_reason, completed_at, receipt_closed_at FROM trips WHERE order_id = $1`, orderID)
}

func (s *Store) ActiveByDriver(ctx context.Context, driverID string) (domain.Trip, error) {
	return s.query(ctx, `
		SELECT id, order_id, driver_id, rider_id, status, rating, cancelled_by, cancel_reason, completed_at, receipt_closed_at FROM trips
		WHERE driver_id = $1 AND status IN ('ASSIGNED', 'IN_PROGRESS')
		ORDER BY created_at DESC LIMIT 1`, driverID)
}

func (s *Store) query(ctx context.Context, query string, arg string) (domain.Trip, error) {
	var t domain.Trip
	err := s.db.QueryRow(ctx, query, arg).Scan(&t.ID, &t.OrderID, &t.DriverID, &t.RiderID, &t.Status, &t.Rating, &t.CancelledBy, &t.CancelReason, &t.CompletedAt, &t.ReceiptClosedAt)
	if t.CompletedAt != nil {
		u := t.CompletedAt.UTC()
		t.CompletedAt = &u
	}
	if t.ReceiptClosedAt != nil {
		u := t.ReceiptClosedAt.UTC()
		t.ReceiptClosedAt = &u
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Trip{}, domain.ErrTripNotFound
	}
	if err != nil {
		return domain.Trip{}, err
	}
	return t, nil
}

// Route reads where the trip starts and ends; the ends are nil for trips
// created before the route was recorded.
func (s *Store) Route(ctx context.Context, tripID string) (domain.Route, error) {
	var pLat, pLng, dLat, dLng *float64
	err := s.db.QueryRow(ctx, `SELECT pickup_lat, pickup_lng, dropoff_lat, dropoff_lng FROM trips WHERE id = $1`, tripID).
		Scan(&pLat, &pLng, &dLat, &dLng)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Route{}, domain.ErrTripNotFound
	}
	if err != nil {
		return domain.Route{}, err
	}
	var r domain.Route
	if pLat != nil && pLng != nil {
		r.Pickup = &domain.LatLng{Lat: *pLat, Lng: *pLng}
	}
	if dLat != nil && dLng != nil {
		r.Dropoff = &domain.LatLng{Lat: *dLat, Lng: *dLng}
	}
	return r, nil
}

// IsCancelled says whether order.cancelled was seen for the order.
func (s *Store) IsCancelled(ctx context.Context, orderID string) (bool, error) {
	return orderIsCancelled(ctx, s.db, orderID)
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func orderIsCancelled(ctx context.Context, db queryRower, orderID string) (bool, error) {
	var cancelled bool
	err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cancelled_orders WHERE order_id = $1)`, orderID).Scan(&cancelled)
	if err != nil {
		return false, fmt.Errorf("check cancelled order %s: %w", orderID, err)
	}
	return cancelled, nil
}

// CloseReceipt marks the receipt of a completed trip as closed and returns the
// time it was closed. The first call sets it; later calls (and a rating that
// already closed it) leave it as it was. ok is false if the trip is not COMPLETED.
func (s *Store) CloseReceipt(ctx context.Context, tripID string) (at time.Time, ok bool, err error) {
	err = s.db.QueryRow(ctx, `
		UPDATE trips SET receipt_closed_at = COALESCE(receipt_closed_at, now())
		WHERE id = $1 AND status = 'COMPLETED'
		RETURNING receipt_closed_at`, tripID).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	return at.UTC(), err == nil, err
}
