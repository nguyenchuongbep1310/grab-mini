package app

import (
	"context"
	"time"

	"grab-mini/payment-service/internal/domain"
)

// RiderPayment is what a rider sees of a charge: what they were charged.
type RiderPayment struct {
	ID        string
	Kind      string
	Status    string
	Amount    int64
	Currency  string
	CreatedAt time.Time
}

// DriverPayout is what a driver sees of the same charge: what they earn from it,
// and the commission taken, never anything about the rider.
type DriverPayout struct {
	ID         string
	Kind       string
	Status     string
	Amount     int64 // the driver's share
	Gross      int64 // what the rider was charged
	Commission int64 // the platform's share
	Currency   string
	CreatedAt  time.Time
}

// KindTotal is a driver's captured payments of one kind.
type KindTotal struct {
	Kind     string
	Driver   int64
	Platform int64
	Count    int
}

// Earnings is a driver's summary. Amounts are the driver's share (after the
// platform's commission) of captured payments, in VND.
type Earnings struct {
	DriverID         string
	Currency         string
	From             *time.Time
	To               *time.Time
	Total            int64
	Fares            int64
	Tips             int64
	CancellationFees int64
	Commission       int64
	Trips            int
	// PayableBalance is what the platform owes the driver now, from the ledger
	// (all time, whatever From/To say).
	PayableBalance int64
}

// CancellationPolicy is what a rider pays for cancelling after a driver was
// assigned, so the app can say so before the rider taps "cancel". The fee applies
// only when the rider cancels more than GraceSeconds after the assignment; 0
// means cancelling is free.
type CancellationPolicy struct {
	Currency     string
	Fee          int64
	GraceSeconds int64
}

func (s *Service) CancellationPolicy() CancellationPolicy {
	return CancellationPolicy{
		Currency:     domain.CurrencyVND,
		Fee:          s.policy.CancellationFee,
		GraceSeconds: int64(s.policy.CancellationGrace / time.Second),
	}
}

// RiderPayments lists the charges of a rider's own trip; total sums the captured ones.
func (s *Service) RiderPayments(ctx context.Context, tripID, riderID string) (payments []RiderPayment, total int64, err error) {
	payments, err = s.reader.RiderPayments(ctx, tripID, riderID)
	if err != nil {
		return nil, 0, err
	}
	for _, p := range payments {
		if p.Status == domain.StatusCaptured {
			total += p.Amount
		}
	}
	return payments, total, nil
}

// DriverPayouts lists the captured payouts of a driver's own trip; total sums them.
func (s *Service) DriverPayouts(ctx context.Context, tripID, driverID string) (payouts []DriverPayout, total int64, err error) {
	payouts, err = s.reader.DriverPayouts(ctx, tripID, driverID)
	if err != nil {
		return nil, 0, err
	}
	for _, p := range payouts {
		total += p.Amount
	}
	return payouts, total, nil
}

// DriverEarnings sums a driver's captured payments in [from, to) and reads their
// payable balance from the ledger.
func (s *Service) DriverEarnings(ctx context.Context, driverID string, from, to *time.Time) (Earnings, error) {
	e := Earnings{DriverID: driverID, Currency: domain.CurrencyVND, From: from, To: to}
	totals, err := s.reader.EarningsByKind(ctx, driverID, from, to)
	if err != nil {
		return e, err
	}
	for _, k := range totals {
		e.Total += k.Driver
		e.Commission += k.Platform
		switch k.Kind {
		case domain.KindFare:
			e.Fares, e.Trips = k.Driver, k.Count
		case domain.KindTip:
			e.Tips = k.Driver
		case domain.KindCancellationFee:
			e.CancellationFees = k.Driver
		}
	}
	e.PayableBalance, err = s.reader.PayableBalance(ctx, driverID)
	return e, err
}
