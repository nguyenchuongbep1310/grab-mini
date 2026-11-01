package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"grab-mini/payment-service/internal/domain"
)

// SettleTrip charges a COMPLETED trip its fare, and a trip CANCELLED by the
// rider the cancellation fee (if one is configured).
func (s *Service) SettleTrip(ctx context.Context, evt TripStatusChanged) error {
	switch evt.Status {
	case TripCompleted:
		if err := requireTrip(evt.TripID, evt.OrderID, evt.DriverID); err != nil {
			return err
		}
		order, err := s.lookupOrder(ctx, evt.OrderID)
		if err != nil {
			return err
		}
		c, skip := s.policy.Fare(order.Net())
		if skip != "" {
			return s.skip(skip, evt.TripID, "fare")
		}
		return s.settle(ctx, settlement{
			eventID: evt.EventID, tripID: evt.TripID, orderID: evt.OrderID,
			riderID: firstNonEmpty(evt.RiderID, order.RiderID), driverID: evt.DriverID, Charge: c,
		})

	case TripCancelled:
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
		assignedFor, err := s.assignedFor(ctx, evt.OrderID, cancelledAt)
		if err != nil {
			return err
		}
		order, err := s.lookupOrder(ctx, evt.OrderID)
		if err != nil {
			return err
		}
		c, skip := s.policy.Cancellation(evt.CancelledBy, order.Net(), assignedFor)
		if skip != "" {
			return s.skip(skip, evt.TripID, "cancellation")
		}
		return s.settle(ctx, settlement{
			eventID: evt.EventID, tripID: evt.TripID, orderID: evt.OrderID,
			riderID: firstNonEmpty(evt.RiderID, order.RiderID), driverID: evt.DriverID, Charge: c,
		})

	}
	return nil // IN_PROGRESS: nothing to settle yet
}

// SettleTip charges the rider's tip and pays all of it to the driver. The
// tip is a separate charge from the fare, so a fare that was declined does not
// block it, nor the reverse.
func (s *Service) SettleTip(ctx context.Context, evt TripRated) error {
	if err := requireTrip(evt.TripID, evt.OrderID, evt.DriverID); err != nil {
		return err
	}
	if evt.RiderID == "" {
		return &InvalidEventError{"trip.rated without riderId"}
	}
	if evt.Currency != "" && evt.Currency != domain.CurrencyVND {
		return &InvalidEventError{fmt.Sprintf("tip currency %q, only %s is supported", evt.Currency, domain.CurrencyVND)}
	}
	if err := domain.CheckAmount(evt.TipAmount); err != nil {
		return err
	}
	c, skip := s.policy.Tip(evt.TipAmount)
	if skip != "" {
		return s.skip(skip, evt.TripID, "tip")
	}
	return s.settle(ctx, settlement{
		eventID: evt.EventID, tripID: evt.TripID, orderID: evt.OrderID,
		riderID: evt.RiderID, driverID: evt.DriverID, Charge: c,
	})
}

func requireTrip(tripID, orderID, driverID string) error {
	if tripID == "" || orderID == "" || driverID == "" {
		return &InvalidEventError{"event without tripId, orderId or driverId"}
	}
	return nil
}

func (s *Service) skip(reason, tripID, what string) error {
	s.metrics.Skipped(reason)
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

type settlement struct {
	eventID  string
	tripID   string
	orderID  string
	riderID  string
	driverID string
	domain.Charge
}

func (st settlement) idempotencyKey() string {
	return strings.ToLower(st.Kind) + ":" + st.tripID
}

// validateTxn checks a ledger transaction can be booked and counts an unbalanced one.
func (s *Service) validateTxn(t domain.Txn) error {
	err := t.Validate()
	s.countUnbalanced(err)
	return err
}

func (s *Service) countUnbalanced(err error) {
	var ub *domain.UnbalancedError
	if errors.As(err, &ub) {
		s.metrics.LedgerUnbalanced()
	}
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
func (s *Service) settle(ctx context.Context, st settlement) error {
	exists, err := s.payments.Exists(ctx, st.tripID, st.Kind)
	if err != nil {
		return err
	}
	if exists {
		s.metrics.Duplicate(st.Kind)
		log.Printf("%s for trip %s already recorded, skipping event %s", st.Kind, st.tripID, st.eventID)
		return nil
	}

	// The ledger transaction is built and checked before the provider is
	// asked for money: an unbalanced one (a bug) must never follow a capture.
	txnID := uuid.NewString()
	txn := domain.ChargeTxn(txnID, st.idempotencyKey(), st.Kind, st.riderID, st.driverID, st.Driver, st.Platform)
	if err := s.validateTxn(txn); err != nil {
		return err
	}

	res, err := s.provider.Charge(ctx, ChargeRequest{
		IdempotencyKey: st.idempotencyKey(),
		RiderID:        st.riderID,
		Amount:         st.Total,
		Currency:       domain.CurrencyVND,
		Description:    st.Kind + " for trip " + st.tripID,
	})
	if err != nil {
		s.metrics.ProviderError()
		return fmt.Errorf("charge %s: %w", st.idempotencyKey(), err)
	}
	status := domain.StatusCaptured
	if res.Status == ChargeDeclined {
		status = domain.StatusDeclined
	} else if res.Status != ChargeCaptured {
		return fmt.Errorf("provider %s returned unknown status %q for %s", s.provider.Name(), res.Status, st.idempotencyKey())
	}

	paymentID := uuid.NewString()
	rec := PaymentRecorded{
		EventID: uuid.NewString(), OccurredAt: s.now().UTC().Format(time.RFC3339),
		PaymentID: paymentID, TripID: st.tripID, OrderID: st.orderID, RiderID: st.riderID, DriverID: st.driverID,
		Kind: st.Kind, Status: status, Amount: st.Total, Currency: domain.CurrencyVND,
	}
	var ledgerID *string
	var driverAmount, platformAmount int64
	if status == domain.StatusCaptured {
		driverAmount, platformAmount = st.Driver, st.Platform
		ledgerID = &txnID
		rec.DriverAmount, rec.PlatformAmount, rec.LedgerTransactionID = driverAmount, platformAmount, txnID
	}
	// Encoded outside the transaction: it may call Schema Registry.
	payload, err := s.enc.EncodePaymentRecorded(ctx, rec)
	if err != nil {
		return err
	}

	payment := domain.Payment{
		ID: paymentID, TripID: st.tripID, OrderID: st.orderID, RiderID: st.riderID, DriverID: st.driverID,
		Kind: st.Kind, Status: status, Amount: st.Total, DriverAmount: driverAmount, PlatformAmount: platformAmount,
		Currency: domain.CurrencyVND, Provider: s.provider.Name(), ProviderRef: res.Ref, DeclineReason: res.DeclineReason,
		LedgerTxnID: ledgerID, SourceEventID: st.eventID,
	}
	recorded := false
	err = s.uow.Do(ctx, func(tx Tx) error {
		first, err := markEventProcessed(ctx, tx, st.eventID)
		if err != nil {
			return err
		}
		if !first {
			s.metrics.Duplicate(st.Kind)
			log.Printf("duplicate event %s, skipping", st.eventID)
			return nil
		}
		inserted, err := tx.InsertPayment(ctx, payment)
		if err != nil {
			return err
		}
		if !inserted {
			// Another event (or instance) recorded this charge between our check
			// and now. Commit the dedupe row and write nothing else.
			s.metrics.Duplicate(st.Kind)
			return nil
		}
		if ledgerID != nil {
			if err := tx.PostLedger(ctx, txn); err != nil {
				return err
			}
		}
		if err := tx.EnqueuePaymentRecorded(ctx, st.tripID, payload); err != nil {
			return err
		}
		recorded = true
		return nil
	})
	if err != nil {
		s.countUnbalanced(err)
		return err
	}
	if !recorded {
		return nil
	}
	s.metrics.Recorded(st.Kind, status)
	if status == domain.StatusCaptured {
		s.metrics.CapturedAmount(st.Kind, st.Total)
		log.Printf("captured %s %d VND for trip %s (driver %d, platform %d)", st.Kind, st.Total, st.tripID, driverAmount, platformAmount)
	} else {
		log.Printf("%s %d VND for trip %s declined: %s", st.Kind, st.Total, st.tripID, res.DeclineReason)
	}
	return nil
}

// markEventProcessed records that eventID was applied, and reports whether
// this is the first time. It must run in the transaction that applies it.
func markEventProcessed(ctx context.Context, tx Tx, eventID string) (bool, error) {
	if _, err := uuid.Parse(eventID); err != nil {
		return false, &InvalidEventError{"eventId " + eventID + " is not a UUID"}
	}
	return tx.MarkEventProcessed(ctx, eventID)
}
