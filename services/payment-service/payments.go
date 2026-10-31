package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/events"
	"grab-mini/pkg/migrate"
	"grab-mini/pkg/outbox"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	serviceName  = "payment-service"
	outboxSource = "payment-service"
)

var (
	paymentsRecorded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_recorded_total",
		Help: "Payments written to the ledger side (and announced), by kind (FARE, TIP, CANCELLATION_FEE) and status (CAPTURED, DECLINED).",
	}, []string{"kind", "status"})

	paymentsCapturedAmount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_captured_amount_total",
		Help: "Sum of captured payments in VND (minor units), by kind.",
	}, []string{"kind"})

	paymentsDuplicate = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_duplicate_total",
		Help: "Events for a charge that was already recorded (redelivery or replay): nothing was charged again.",
	}, []string{"kind"})

	paymentsSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_skipped_total",
		Help: "Trip events that produced no charge, by reason (no_price, zero_amount, fee_disabled, driver_cancel, unknown_actor).",
	}, []string{"reason"})

	providerErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payment_provider_errors_total",
		Help: "Charges the payment provider could not answer (the event is retried; the idempotency key makes that safe).",
	})
)

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `payment-service migrate`).
func connectDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate.AtStartup(ctx, pool, serviceName, migrationFiles); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// runMigrate is the `payment-service migrate` command: apply pending migrations and exit.
func runMigrate(env Env) {
	ctx := context.Background()
	pool, err := openPool(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	n, err := migrate.Up(ctx, pool, serviceName, migrationFiles)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d migration(s) applied", serviceName, n)
}

// encoder turns an event into its Avro wire bytes (*eventcodec.Codec).
type encoder interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

// service holds what the handlers share.
type service struct {
	db       *pgxpool.Pool
	provider PaymentProvider
	policy   Policy
	enc      encoder
	relay    *outbox.Relay // may be nil in tests that don't publish
	now      func() time.Time
}

func newService(db *pgxpool.Pool, provider PaymentProvider, policy Policy, enc encoder, relay *outbox.Relay) *service {
	return &service{db: db, provider: provider, policy: policy, enc: enc, relay: relay, now: time.Now}
}

// invalidEvent is an event that can never be charged (no driver, a negative
// tip, a currency we don't handle). Permanent: straight to the dead-letter
// topic instead of being retried.
type invalidEvent struct{ reason string }

func (e *invalidEvent) Error() string   { return "invalid payment event: " + e.reason }
func (e *invalidEvent) Permanent() bool { return true }

// errOrderUnknown asks for a retry: the trip event arrived before order.created
// was recorded (they are different topics, so there is no order between them).
// If the order never shows up the event ends in the dead-letter topic, which
// pages: a finished ride without a price is a ride nobody can charge.
type errOrderUnknown struct{ orderID string }

func (e *errOrderUnknown) Error() string {
	return "no price recorded yet for order " + e.orderID + " (order.created not seen); retrying"
}

// ----------------------------------------------------------- order prices ----

// recordOrder keeps the price locked at booking, which is what the rider is
// charged when the trip completes. order.created is the source because it is
// the one event that carries the price and the rider together; the table is a
// local copy so a trip event never needs a call to Order Service. A redelivery
// hits the primary key and changes nothing.
func (s *service) recordOrder(ctx context.Context, evt events.OrderCreated) error {
	if evt.OrderID == "" || evt.RiderID == "" {
		return &invalidEvent{"order.created without orderId or riderId"}
	}
	var price *int64
	if evt.Price != nil {
		p, err := priceToMinor(*evt.Price)
		if err != nil {
			return err
		}
		price = &p
	}
	_, err := s.db.Exec(ctx,
		`INSERT INTO order_prices (order_id, rider_id, price) VALUES ($1, $2, $3) ON CONFLICT (order_id) DO NOTHING`,
		evt.OrderID, evt.RiderID, price)
	return err
}

type orderInfo struct {
	riderID string
	price   *int64 // nil: the order has no price
}

func (s *service) lookupOrder(ctx context.Context, orderID string) (orderInfo, error) {
	var o orderInfo
	err := s.db.QueryRow(ctx, `SELECT rider_id, price FROM order_prices WHERE order_id = $1`, orderID).Scan(&o.riderID, &o.price)
	if errors.Is(err, pgx.ErrNoRows) {
		return orderInfo{}, &errOrderUnknown{orderID}
	}
	return o, err
}

// ----------------------------------------------------------------- events ----

// onTripStatus charges a COMPLETED trip its fare, and a trip CANCELLED by the
// rider the cancellation fee (if one is configured).
func (s *service) onTripStatus(ctx context.Context, evt events.TripStatusChanged) error {
	switch evt.Status {
	case events.TripStatusCompleted:
		if err := requireTrip(evt.TripID, evt.OrderID, evt.DriverID); err != nil {
			return err
		}
		order, err := s.lookupOrder(ctx, evt.OrderID)
		if err != nil {
			return err
		}
		c, skip := s.policy.Fare(order.price)
		if skip != "" {
			return s.skip(skip, evt.TripID, "fare")
		}
		return s.settle(ctx, settlement{
			eventID: evt.EventID, tripID: evt.TripID, orderID: evt.OrderID,
			riderID: firstNonEmpty(evt.RiderID, order.riderID), driverID: evt.DriverID, charge: c,
		})

	case events.TripStatusCancelled:
		// Decided on who cancelled and the fee setting alone first, so the
		// common case (nothing to charge) doesn't depend on the order being known.
		if skip := s.policy.CancellationApplies(evt.CancelledBy); skip != "" {
			return s.skip(skip, evt.TripID, "cancellation")
		}
		if err := requireTrip(evt.TripID, evt.OrderID, evt.DriverID); err != nil {
			return err
		}
		cancelledAt, err := parseEventTime(evt.OccurredAt)
		if err != nil {
			return err
		}
		assignedFor, err := s.assignedFor(ctx, evt.TripID, cancelledAt)
		if err != nil {
			return err
		}
		order, err := s.lookupOrder(ctx, evt.OrderID)
		if err != nil {
			return err
		}
		c, skip := s.policy.Cancellation(evt.CancelledBy, order.price, assignedFor)
		if skip != "" {
			return s.skip(skip, evt.TripID, "cancellation")
		}
		return s.settle(ctx, settlement{
			eventID: evt.EventID, tripID: evt.TripID, orderID: evt.OrderID,
			riderID: firstNonEmpty(evt.RiderID, order.riderID), driverID: evt.DriverID, charge: c,
		})

	case events.TripStatusAssigned:
		// Only remembered: the cancellation fee needs to know how long the driver
		// had been committed. The first ASSIGNED wins; a redelivery changes nothing.
		if evt.TripID == "" {
			return &invalidEvent{"trip.status.changed without tripId"}
		}
		at, err := parseEventTime(evt.OccurredAt)
		if err != nil {
			return err
		}
		_, err = s.db.Exec(ctx,
			`INSERT INTO trip_assignments (trip_id, assigned_at) VALUES ($1, $2) ON CONFLICT (trip_id) DO NOTHING`,
			evt.TripID, at)
		return err
	}
	return nil // IN_PROGRESS: nothing to settle yet
}

// onTripRated charges the rider's tip and pays all of it to the driver. The
// tip is a separate charge from the fare, so a fare that was declined does not
// block it, nor the reverse.
func (s *service) onTripRated(ctx context.Context, evt events.TripRated) error {
	if err := requireTrip(evt.TripID, evt.OrderID, evt.DriverID); err != nil {
		return err
	}
	if evt.RiderID == "" {
		return &invalidEvent{"trip.rated without riderId"}
	}
	if evt.Currency != "" && evt.Currency != currencyVND {
		return &invalidEvent{fmt.Sprintf("tip currency %q, only %s is supported", evt.Currency, currencyVND)}
	}
	if err := checkAmount(evt.TipAmount); err != nil {
		return err
	}
	c, skip := s.policy.Tip(evt.TipAmount)
	if skip != "" {
		return s.skip(skip, evt.TripID, "tip")
	}
	return s.settle(ctx, settlement{
		eventID: evt.EventID, tripID: evt.TripID, orderID: evt.OrderID,
		riderID: evt.RiderID, driverID: evt.DriverID, charge: c,
	})
}

func requireTrip(tripID, orderID, driverID string) error {
	if tripID == "" || orderID == "" || driverID == "" {
		return &invalidEvent{"event without tripId, orderId or driverId"}
	}
	return nil
}

func (s *service) skip(reason, tripID, what string) error {
	paymentsSkipped.WithLabelValues(reason).Inc()
	log.Printf("no %s charge for trip %s: %s", what, tripID, reason)
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ------------------------------------------------------------- settlement ----

type settlement struct {
	eventID  string
	tripID   string
	orderID  string
	riderID  string
	driverID string
	charge
}

func (st settlement) idempotencyKey() string {
	return strings.ToLower(st.Kind) + ":" + st.tripID
}

// settle charges the rider through the provider and books the result. It can
// be retried or redelivered at any point and converges on exactly one payment
// per (trip, kind):
//   - a charge already recorded is answered without calling the provider;
//   - the provider call is idempotent on the key, so a retry after a crash
//     between "provider captured" and "row committed" does not charge twice;
//   - the eventId dedupe row, the payment, the ledger entries and the outbox
//     event commit in one transaction, so "seen" never outlives a payment that
//     failed to save, and a payment that is saved is always announced.
func (s *service) settle(ctx context.Context, st settlement) error {
	var exists bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM payments WHERE trip_id = $1 AND kind = $2)`,
		st.tripID, st.Kind).Scan(&exists); err != nil {
		return err
	}
	if exists {
		paymentsDuplicate.WithLabelValues(st.Kind).Inc()
		log.Printf("%s for trip %s already recorded, skipping event %s", st.Kind, st.tripID, st.eventID)
		return nil
	}

	// The ledger transaction is built and checked before the provider is
	// asked for money: an unbalanced one (a bug) must never follow a capture.
	txnID := uuid.NewString()
	txn := chargeTxn(txnID, st.idempotencyKey(), st.Kind, st.riderID, st.driverID, st.Driver, st.Platform)
	if err := txn.Validate(); err != nil {
		return err
	}

	res, err := s.provider.Charge(ctx, ChargeRequest{
		IdempotencyKey: st.idempotencyKey(),
		RiderID:        st.riderID,
		Amount:         st.Total,
		Currency:       currencyVND,
		Description:    st.Kind + " for trip " + st.tripID,
	})
	if err != nil {
		providerErrors.Inc()
		return fmt.Errorf("charge %s: %w", st.idempotencyKey(), err)
	}
	status := events.PaymentStatusCaptured
	if res.Status == ChargeDeclined {
		status = events.PaymentStatusDeclined
	} else if res.Status != ChargeCaptured {
		return fmt.Errorf("provider %s returned unknown status %q for %s", s.provider.Name(), res.Status, st.idempotencyKey())
	}

	paymentID := uuid.NewString()
	rec := events.PaymentRecorded{
		Envelope:  events.Envelope{EventID: uuid.NewString(), OccurredAt: s.now().UTC().Format(time.RFC3339)},
		PaymentID: paymentID, TripID: st.tripID, OrderID: st.orderID, RiderID: st.riderID, DriverID: st.driverID,
		Kind: st.Kind, Status: status, Amount: st.Total, Currency: currencyVND,
	}
	var ledgerID *string
	var driverAmount, platformAmount int64
	if status == events.PaymentStatusCaptured {
		driverAmount, platformAmount = st.Driver, st.Platform
		ledgerID = &txnID
		rec.DriverAmount, rec.PlatformAmount, rec.LedgerTransactionID = driverAmount, platformAmount, txnID
	}
	// Encoded outside the transaction: it may call Schema Registry.
	payload, err := s.enc.Encode(ctx, events.TopicPaymentRecorded, rec)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	first, err := markEventProcessed(ctx, tx, st.eventID)
	if err != nil {
		return err
	}
	if !first {
		paymentsDuplicate.WithLabelValues(st.Kind).Inc()
		log.Printf("duplicate event %s, skipping", st.eventID)
		return nil
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO payments (id, trip_id, order_id, rider_id, driver_id, kind, status, amount, driver_amount, platform_amount,
		                      currency, provider, provider_ref, decline_reason, ledger_txn_id, source_event_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (trip_id, kind) DO NOTHING`,
		paymentID, st.tripID, st.orderID, st.riderID, st.driverID, st.Kind, status, st.Total, driverAmount, platformAmount,
		currencyVND, s.provider.Name(), res.Ref, res.DeclineReason, ledgerID, st.eventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Another event (or instance) recorded this charge between our check
		// and now. Commit the dedupe row and write nothing else.
		paymentsDuplicate.WithLabelValues(st.Kind).Inc()
		return tx.Commit(ctx)
	}
	if ledgerID != nil {
		if err := post(ctx, tx, txn); err != nil {
			return err
		}
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicPaymentRecorded, []byte(st.tripID), payload); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.relay != nil {
		s.relay.Wake()
	}
	paymentsRecorded.WithLabelValues(st.Kind, status).Inc()
	if status == events.PaymentStatusCaptured {
		paymentsCapturedAmount.WithLabelValues(st.Kind).Add(float64(st.Total))
		log.Printf("captured %s %d VND for trip %s (driver %d, platform %d)", st.Kind, st.Total, st.tripID, driverAmount, platformAmount)
	} else {
		log.Printf("%s %d VND for trip %s declined: %s", st.Kind, st.Total, st.tripID, res.DeclineReason)
	}
	return nil
}

// markEventProcessed records that eventID was applied, and reports whether
// this is the first time. It must run in the transaction that applies it.
func markEventProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	if _, err := uuid.Parse(eventID); err != nil {
		return false, &invalidEvent{"eventId " + eventID + " is not a UUID"}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO payment_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// parseEventTime reads an event's occurredAt. A missing or malformed time is a
// permanent error: no amount of retrying will make it parse.
func parseEventTime(v string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, &invalidEvent{fmt.Sprintf("occurredAt %q is not an RFC 3339 time", v)}
	}
	return t, nil
}

// assignedFor is how long the trip's driver had been assigned at cancelledAt, or
// nil when the ASSIGNED event was never recorded (this service was not yet
// consuming when it was published). Events for one trip share a partition, so
// within a running service the ASSIGNED event is always handled first.
func (s *service) assignedFor(ctx context.Context, tripID string, cancelledAt time.Time) (*time.Duration, error) {
	var assignedAt time.Time
	err := s.db.QueryRow(ctx, `SELECT assigned_at FROM trip_assignments WHERE trip_id = $1`, tripID).Scan(&assignedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d := cancelledAt.Sub(assignedAt)
	return &d, nil
}
