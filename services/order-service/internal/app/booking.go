package app

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/domain"
)

// booking is a CreateOrder in progress: what has been settled so far and
// what has to be given back if the order is not saved.
type booking struct {
	order   NewOrder
	payload []byte
	body    []byte
	// priced: a quote may be claimed (Pricer.Release); promo: a token may be
	// claimed (PromoChecker.Release). saved: the order committed, keep both.
	settled      Pricing
	applied      Applied
	priced       bool
	promoApplied bool
	saved        bool
}

// statusCreated is the status of a new booking (HTTP 201), stored with its key.
const statusCreated = 201

// errReplayed rolls back the transaction of a request answered from its key.
var errReplayed = errors.New("answered from the idempotency key")

// CreateOrder books an order and queues order.created with it.
//
// With an Idempotency-Key one transaction opens first: the key is locked and
// looked up, and a retry is answered from what the first attempt stored
// before the quote is looked at (a retry carries the quote the first attempt
// used up). The key stays locked until the transaction ends, so a concurrent
// duplicate waits and then replays rather than booking twice. Pricing, the
// promo and the event then run inside that transaction, and the order, its
// event and the key's record commit together. Without a key, pricing and the
// event come first and a transaction covers only the insert.
//
// Once a quote or promo token is claimed, any failure gives it back so the
// rider's retry can use it.
func (s *Service) CreateOrder(ctx context.Context, cmd CreateOrderCommand) (CreateOrderResult, error) {
	riderID, key, req := cmd.RiderID, cmd.IdempotencyKey, cmd.Request
	fp := req.Fingerprint()
	b := &booking{}
	defer func() {
		if !b.saved {
			s.release(ctx, b)
		}
	}()

	if key == "" {
		if err := s.prepare(ctx, riderID, req, b); err != nil {
			return CreateOrderResult{}, err
		}
		if err := s.uow.Do(ctx, func(tx Tx) error { return s.insert(ctx, tx, b, riderID, nil) }); err != nil {
			log.Printf("failed to save order: %v", err)
			return CreateOrderResult{}, ErrInternal
		}
		return s.created(b, false), nil
	}

	var (
		res                         CreateOrderResult
		entered, answered, inserted bool
		saveErr                     error
	)
	err := s.uow.Do(ctx, func(tx Tx) error {
		entered = true
		var rec *domain.KeyRecord
		err := tx.LockKey(ctx, riderID, key)
		if err == nil {
			rec, err = tx.Lookup(ctx, riderID, key)
		}
		if err != nil {
			log.Printf("failed to look up idempotency key: %v", err)
			return ErrInternal
		}
		if rec != nil {
			answered = true
			if res, err = s.answerFromKey(rec, fp); err != nil {
				return err
			}
			return errReplayed
		}
		// Whatever fails from here, the claims go back while the transaction
		// (and with it the key's lock) is still open: Do rolls back before it
		// returns, and a waiting retry with this key must not find the quote
		// still claimed. The flags are cleared so the deferred release in
		// CreateOrder does not give them back twice.
		if err := s.settle(ctx, tx, riderID, key, fp, req, b, &saveErr); err != nil {
			s.release(ctx, b)
			b.priced, b.promoApplied = false, false
			return err
		}
		inserted = true
		return nil
	})
	switch {
	case err == nil:
		return s.created(b, true), nil
	case answered && errors.Is(err, errReplayed):
		return res, nil
	case !entered:
		log.Printf("failed to begin transaction: %v", err)
		return CreateOrderResult{}, ErrInternal
	case saveErr == nil && !inserted:
		// Answered from the key, or a failure that was already logged or is the caller's.
		return CreateOrderResult{}, err
	}
	// The save or the commit failed.
	if errors.Is(err, ErrDuplicateKey) {
		// Another request with this key committed first (the lock normally
		// makes it finish before we get here): answer with its order.
		rec, lerr := s.keys.Lookup(ctx, riderID, key)
		if lerr == nil && rec != nil {
			return s.answerFromKey(rec, fp)
		}
	}
	log.Printf("failed to save order: %v", err)
	return CreateOrderResult{}, ErrInternal
}

// settle prices and prepares the booking and inserts it, with the key's
// record, into tx. A failed insert is also stored in saveErr.
func (s *Service) settle(ctx context.Context, tx Tx, riderID, key, fp string, req domain.BookingRequest, b *booking, saveErr *error) error {
	if err := s.prepare(ctx, riderID, req, b); err != nil {
		return err
	}
	idem := &domain.KeyRecord{Key: key, Fingerprint: fp, OrderID: b.order.ID, Status: statusCreated, Body: b.body}
	*saveErr = s.insert(ctx, tx, b, riderID, idem)
	return *saveErr
}

// prepare settles the price and the promo, and encodes the event and the
// receipt, filling b. It touches no database.
func (s *Service) prepare(ctx context.Context, riderID string, req domain.BookingRequest, b *booking) error {
	orderID := uuid.NewString()

	settled, err := s.pricer.Price(ctx, riderID, req)
	var moved *QuoteConflict
	switch {
	case errors.As(err, &moved), errors.Is(err, ErrInvalidQuote):
		return err
	case err != nil:
		log.Printf("failed to price order: %v", err)
		return ErrInternal
	}
	b.settled, b.priced = settled, true

	// A promo token is checked against the price just settled, so a token for
	// another price (the quote expired and the price moved) is refused rather
	// than applied to a different fare.
	var promoCode string
	var discount int64
	if req.PromoToken != "" {
		a, err := s.promos.Apply(ctx, riderID, req.VehicleType, settled.Price, req.PromoToken)
		if err != nil {
			return err
		}
		b.applied, b.promoApplied = a, true
		promoCode, discount = a.Code, a.Discount
	}

	b.order = NewOrder{
		ID: orderID, RiderID: riderID, Pickup: *req.Pickup, Dropoff: *req.Dropoff, VehicleType: req.VehicleType,
		Multiplier: settled.Multiplier, Price: &settled.Price, PromoCode: promoCode, Discount: discount,
	}

	// Encode before touching the database: if the schema registry is down
	// or rejects the schema, no order row is left behind without an event.
	b.payload, err = s.enc.OrderCreated(ctx, OrderCreated{
		OrderID: orderID, RiderID: riderID, Pickup: *req.Pickup, Dropoff: *req.Dropoff, VehicleType: req.VehicleType,
		SurgeMultiplier: settled.Multiplier, Price: &settled.Price, PromoCode: promoCode, Discount: discount,
	})
	if err != nil {
		log.Printf("failed to encode order.created: %v", err)
		return ErrInternal
	}

	b.body = s.receipt(Receipt{
		OrderID: orderID, SurgeMultiplier: settled.Multiplier, Price: settled.Price,
		FinalPrice: settled.Price - float64(discount), Discount: discount, PromoCode: promoCode,
	})
	return nil
}

// insert adds the order, its order.created outbox row and, with an
// Idempotency-Key, the key's record to tx. The caller commits.
func (s *Service) insert(ctx context.Context, tx Tx, b *booking, riderID string, idem *domain.KeyRecord) error {
	if err := tx.InsertOrder(ctx, b.order); err != nil {
		return err
	}
	if err := tx.QueueOrderCreated(ctx, b.order.ID, b.payload); err != nil {
		return err
	}
	if idem != nil {
		return tx.SaveKey(ctx, riderID, *idem)
	}
	return nil
}

// created marks b saved, wakes the relay and counts the order. Only after a commit.
func (s *Service) created(b *booking, withKey bool) CreateOrderResult {
	b.saved = true
	s.relay.Wake()
	s.metrics.OrderCreated(string(b.order.VehicleType))
	if withKey {
		s.metrics.Idempotent("new")
	}
	return CreateOrderResult{Status: statusCreated, Body: b.body}
}

// release gives back the promo token and the quote a failed booking claimed.
func (s *Service) release(ctx context.Context, b *booking) {
	if b.promoApplied {
		s.promos.Release(ctx, b.applied)
	}
	if b.priced {
		s.pricer.Release(ctx, b.settled)
	}
}

// answerFromKey is the answer to a request whose key has a record: the
// original response for the same body, ErrKeyMismatch for another.
func (s *Service) answerFromKey(rec *domain.KeyRecord, fp string) (CreateOrderResult, error) {
	if rec.Fingerprint != fp {
		s.metrics.Idempotent("mismatch")
		return CreateOrderResult{}, ErrKeyMismatch
	}
	s.metrics.Idempotent("replayed")
	return CreateOrderResult{Status: rec.Status, Body: rec.Body, Replayed: true}, nil
}
