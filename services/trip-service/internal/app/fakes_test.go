package app

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"grab-mini/trip-service/internal/domain"
)

// world is an in-memory stand-in for every port: the trips and the order marks
// (with a unit of work that really rolls back), the outbox, the driver status
// record, driver positions, the encoder and the counters.
type world struct {
	trips     map[string]domain.Trip
	routes    map[string]domain.Route
	cancelled map[string]bool // orders the rider cancelled
	processed map[string]bool // event ids
	outbox    []queued

	driverStatus map[string]domain.DriverStatus
	positions    map[string]domain.Position
	locErr       error
	routeErr     error
	setErr       error
	encodeErr    error
	saveErr      error

	transitioned []string
	rated        int
	refused      []string // "step/reason"

	// beforeLock runs inside the unit of work, once, right after the order lock:
	// it lets a test have "another consumer" act in the middle of a transaction.
	beforeLock func()
}

type queued struct {
	Topic, Key string
	Payload    string
}

func newWorld() *world {
	return &world{
		trips: map[string]domain.Trip{}, routes: map[string]domain.Route{},
		cancelled: map[string]bool{}, processed: map[string]bool{},
		driverStatus: map[string]domain.DriverStatus{}, positions: map[string]domain.Position{},
	}
}

func (w *world) add(t domain.Trip) domain.Trip { w.trips[t.ID] = t; return t }

// service builds the Service on the world, with a fixed clock.
func (w *world) service(prox domain.Proximity, now time.Time) *Service {
	return NewService(Deps{
		Trips: w, Routes: w, Orders: w, Receipts: w, UnitOfWork: w, Drivers: fakeDrivers{w}, Locations: w,
		Encoder: w, Metrics: w, Proximity: prox, Now: func() time.Time { return now },
	})
}

// ---- reads

func (w *world) ByID(_ context.Context, id string) (domain.Trip, error) {
	if t, ok := w.trips[id]; ok {
		return t, nil
	}
	return domain.Trip{}, domain.ErrTripNotFound
}

func (w *world) find(match func(domain.Trip) bool) (domain.Trip, error) {
	for _, id := range slices.Sorted(maps.Keys(w.trips)) {
		if t := w.trips[id]; match(t) {
			return t, nil
		}
	}
	return domain.Trip{}, domain.ErrTripNotFound
}

func (w *world) ByOrderID(_ context.Context, orderID string) (domain.Trip, error) {
	return w.find(func(t domain.Trip) bool { return t.OrderID == orderID })
}

func (w *world) ActiveByDriver(_ context.Context, driverID string) (domain.Trip, error) {
	return w.find(func(t domain.Trip) bool {
		return t.DriverID == driverID && (t.Status == domain.Assigned || t.Status == domain.InProgress)
	})
}

func (w *world) Route(_ context.Context, id string) (domain.Route, error) {
	return w.routes[id], w.routeErr
}

func (w *world) IsCancelled(_ context.Context, orderID string) (bool, error) {
	return w.cancelled[orderID], nil
}

func (w *world) CloseReceipt(_ context.Context, id string) (time.Time, bool, error) {
	t, ok := w.trips[id]
	if !ok || t.Status != domain.Completed {
		return time.Time{}, false, nil
	}
	if t.ReceiptClosedAt == nil {
		at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		t.ReceiptClosedAt = &at
		w.trips[id] = t
	}
	return *t.ReceiptClosedAt, true, nil
}

// ---- unit of work

func (w *world) Do(_ context.Context, fn func(Tx) error) error {
	trips, cancelled, processed, outbox := maps.Clone(w.trips), maps.Clone(w.cancelled), maps.Clone(w.processed), slices.Clone(w.outbox)
	if err := fn(w); err != nil {
		w.trips, w.cancelled, w.processed, w.outbox = trips, cancelled, processed, outbox
		return err
	}
	return nil
}

func (w *world) LockOrder(context.Context, string) error {
	if w.beforeLock != nil {
		f := w.beforeLock
		w.beforeLock = nil
		f()
	}
	return nil
}
func (w *world) IsOrderCancelled(_ context.Context, orderID string) (bool, error) {
	return w.cancelled[orderID], nil
}
func (w *world) MarkOrderCancelled(_ context.Context, orderID string) error {
	w.cancelled[orderID] = true
	return nil
}
func (w *world) TripIDForOrder(_ context.Context, orderID string) (string, bool, error) {
	t, err := w.find(func(t domain.Trip) bool { return t.OrderID == orderID })
	return t.ID, err == nil, nil
}
func (w *world) MarkEventProcessed(_ context.Context, id string) (bool, error) {
	first := !w.processed[id]
	w.processed[id] = true
	return first, nil
}
func (w *world) CreateTrip(_ context.Context, t domain.Trip, r domain.Route) (bool, error) {
	if _, err := w.find(func(x domain.Trip) bool { return x.OrderID == t.OrderID }); err == nil {
		return false, nil
	}
	w.trips[t.ID] = t
	w.routes[t.ID] = r
	return true, nil
}
func (w *world) Transition(_ context.Context, id string, a domain.Action, c domain.CancelInfo) (bool, error) {
	t, ok := w.trips[id]
	if !ok || !a.CanStartFrom(t.Status) {
		return false, nil
	}
	t.Status = a.To()
	if a.To() == domain.Cancelled {
		t.CancelledBy, t.CancelReason = c.By, c.Reason
	}
	if a.To() == domain.Completed {
		at := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
		t.CompletedAt = &at
	}
	w.trips[id] = t
	return true, nil
}
func (w *world) SaveRating(_ context.Context, id string, r domain.Rating) (bool, error) {
	if w.saveErr != nil {
		return false, w.saveErr
	}
	t, ok := w.trips[id]
	if !ok || t.Status != domain.Completed || t.Rating != nil {
		return false, nil
	}
	t.Rating = &r.Stars
	w.trips[id] = t
	return true, nil
}
func (w *world) EnqueueStatusChanged(_ context.Context, tripID string, p []byte) error {
	w.outbox = append(w.outbox, queued{"trip.status.changed", tripID, string(p)})
	return nil
}
func (w *world) EnqueueTripRated(_ context.Context, driverID string, p []byte) error {
	w.outbox = append(w.outbox, queued{"trip.rated", driverID, string(p)})
	return nil
}

// ---- drivers, locations

// fakeDrivers is the Drivers port (its Transition would clash with the Tx's).
type fakeDrivers struct{ w *world }

func (d fakeDrivers) Set(_ context.Context, id string, s domain.DriverStatus) error {
	if d.w.setErr != nil {
		return d.w.setErr
	}
	d.w.driverStatus[id] = s
	return nil
}

func (d fakeDrivers) Transition(_ context.Context, id string, from, to domain.DriverStatus) (bool, error) {
	if d.w.driverStatus[id] != from {
		return false, nil
	}
	d.w.driverStatus[id] = to
	return true, nil
}

func (w *world) Last(_ context.Context, id string) (domain.Position, bool, error) {
	if w.locErr != nil {
		return domain.Position{}, false, w.locErr
	}
	p, ok := w.positions[id]
	return p, ok, nil
}

// ---- encoder, metrics

func (w *world) EncodeStatusChanged(_ context.Context, e StatusChanged) ([]byte, error) {
	if w.encodeErr != nil {
		return nil, w.encodeErr
	}
	return []byte(string(e.Status) + "/" + e.CancelledBy + "/" + e.Reason), nil
}
func (w *world) EncodeTripRated(_ context.Context, e TripRated) ([]byte, error) {
	if w.encodeErr != nil {
		return nil, w.encodeErr
	}
	return []byte("rated"), nil
}
func (w *world) Transitioned(s string)             { w.transitioned = append(w.transitioned, s) }
func (w *world) Rated()                            { w.rated++ }
func (w *world) ProximityRefused(step, why string) { w.refused = append(w.refused, step+"/"+why) }

var errBoom = errors.New("boom")
