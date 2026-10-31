package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
)

// routes is the HTTP API. Both routes need a token; the handlers then check
// the caller is the rider or the driver the money belongs to.
func (s *service) routes(verifier *auth.Verifier) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /payments", verifier.Require(http.HandlerFunc(s.handleGetPayments), auth.Rider, auth.Driver))
	mux.Handle("GET /cancellation-policy", verifier.Require(http.HandlerFunc(s.handleCancellationPolicy), auth.Rider, auth.Driver))
	mux.Handle("GET /drivers/{id}/earnings", verifier.Require(http.HandlerFunc(s.handleEarnings), auth.Driver))
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
}

func internalError(w http.ResponseWriter, what string, err error) {
	log.Printf("%s: %v", what, err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
}

// riderPayment is what a rider sees of a charge: what they were charged.
type riderPayment struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Status    string    `json:"status"`
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	CreatedAt time.Time `json:"createdAt"`
}

// driverPayout is what a driver sees of the same charge: what they earn from it,
// and the commission taken, never anything about the rider.
type driverPayout struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Status     string    `json:"status"`
	Amount     int64     `json:"amount"`
	Gross      int64     `json:"gross"`
	Commission int64     `json:"commission"`
	Currency   string    `json:"currency"`
	CreatedAt  time.Time `json:"createdAt"`
}

// handleCancellationPolicy serves GET /cancellation-policy: what a rider pays for cancelling
// after a driver was assigned, so the app can say so before the rider taps "cancel". The fee
// applies only when the rider cancels more than graceSeconds after the assignment; 0 means
// cancelling is free.
func (s *service) handleCancellationPolicy(w http.ResponseWriter, r *http.Request) {
	if _, ok := auth.From(r.Context()); !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"currency":     currencyVND,
		"fee":          s.policy.CancellationFee,
		"graceSeconds": int64(s.policy.CancellationGrace / time.Second),
	})
}

// handleGetPayments serves GET /payments?tripId=. A rider sees the charges of
// their own trip, a driver the payouts of their own: for anyone else the list
// is empty, exactly as for a trip with no payments yet, so the endpoint does
// not reveal whether a trip exists. total sums the captured amounts.
func (s *service) handleGetPayments(w http.ResponseWriter, r *http.Request) {
	id, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	tripID := r.URL.Query().Get("tripId")
	if tripID == "" {
		badRequest(w, "tripId is required")
		return
	}

	if id.Role == auth.Driver {
		payouts, total, err := s.driverPayouts(r.Context(), tripID, id.Subject)
		if err != nil {
			internalError(w, "load payouts", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"tripId": tripID, "currency": currencyVND, "payouts": payouts, "total": total})
		return
	}
	payments, total, err := s.riderPayments(r.Context(), tripID, id.Subject)
	if err != nil {
		internalError(w, "load payments", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tripId": tripID, "currency": currencyVND, "payments": payments, "total": total})
}

func (s *service) riderPayments(ctx context.Context, tripID, riderID string) ([]riderPayment, int64, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, kind, status, amount, currency, created_at FROM payments
		WHERE trip_id = $1 AND rider_id = $2 ORDER BY created_at, kind`, tripID, riderID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out, total := []riderPayment{}, int64(0)
	for rows.Next() {
		var p riderPayment
		if err := rows.Scan(&p.ID, &p.Kind, &p.Status, &p.Amount, &p.Currency, &p.CreatedAt); err != nil {
			return nil, 0, err
		}
		if p.Status == events.PaymentStatusCaptured {
			total += p.Amount
		}
		out = append(out, p)
	}
	return out, total, rows.Err()
}

func (s *service) driverPayouts(ctx context.Context, tripID, driverID string) ([]driverPayout, int64, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, kind, status, driver_amount, amount, platform_amount, currency, created_at FROM payments
		WHERE trip_id = $1 AND driver_id = $2 AND status = 'CAPTURED' ORDER BY created_at, kind`, tripID, driverID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out, total := []driverPayout{}, int64(0)
	for rows.Next() {
		var p driverPayout
		if err := rows.Scan(&p.ID, &p.Kind, &p.Status, &p.Amount, &p.Gross, &p.Commission, &p.Currency, &p.CreatedAt); err != nil {
			return nil, 0, err
		}
		total += p.Amount
		out = append(out, p)
	}
	return out, total, rows.Err()
}

// earnings is a driver's summary. Amounts are the driver's share (after the
// platform's commission) of captured payments, in VND.
type earnings struct {
	DriverID         string     `json:"driverId"`
	Currency         string     `json:"currency"`
	From             *time.Time `json:"from,omitempty"`
	To               *time.Time `json:"to,omitempty"`
	Total            int64      `json:"total"`
	Fares            int64      `json:"fares"`
	Tips             int64      `json:"tips"`
	CancellationFees int64      `json:"cancellationFees"`
	Commission       int64      `json:"commission"`
	Trips            int        `json:"trips"`
	// PayableBalance is what the platform owes the driver now, from the ledger
	// (all time, whatever from/to say).
	PayableBalance int64 `json:"payableBalance"`
}

func (s *service) driverEarnings(ctx context.Context, driverID string, from, to *time.Time) (earnings, error) {
	e := earnings{DriverID: driverID, Currency: currencyVND, From: from, To: to}
	rows, err := s.db.Query(ctx, `
		SELECT kind, COALESCE(SUM(driver_amount), 0)::bigint, COALESCE(SUM(platform_amount), 0)::bigint, COUNT(*)
		FROM payments
		WHERE driver_id = $1 AND status = 'CAPTURED'
		  AND ($2::timestamptz IS NULL OR created_at >= $2) AND ($3::timestamptz IS NULL OR created_at < $3)
		GROUP BY kind`, driverID, from, to)
	if err != nil {
		return e, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var driver, platform int64
		var n int
		if err := rows.Scan(&kind, &driver, &platform, &n); err != nil {
			return e, err
		}
		e.Total += driver
		e.Commission += platform
		switch kind {
		case events.PaymentKindFare:
			e.Fares, e.Trips = driver, n
		case events.PaymentKindTip:
			e.Tips = driver
		case events.PaymentKindCancellationFee:
			e.CancellationFees = driver
		}
	}
	if err := rows.Err(); err != nil {
		return e, err
	}
	err = s.db.QueryRow(ctx, `SELECT COALESCE(SUM(amount), 0)::bigint FROM ledger_entries WHERE account_id = $1`,
		driverPayableAccount(driverID).id).Scan(&e.PayableBalance)
	return e, err
}

// handleEarnings serves GET /drivers/{id}/earnings[?from=&to=] (RFC 3339; from
// inclusive, to exclusive). A driver may read only their own.
func (s *service) handleEarnings(w http.ResponseWriter, r *http.Request) {
	id, ok := auth.From(r.Context())
	if !ok || id.Role != auth.Driver || id.Subject != r.PathValue("id") {
		auth.Deny(w, http.StatusForbidden, "drivers can read only their own earnings")
		return
	}
	from, err := parseTime(r.URL.Query().Get("from"))
	if err != nil {
		badRequest(w, "from must be an RFC 3339 time")
		return
	}
	to, err := parseTime(r.URL.Query().Get("to"))
	if err != nil {
		badRequest(w, "to must be an RFC 3339 time")
		return
	}
	e, err := s.driverEarnings(r.Context(), id.Subject, from, to)
	if err != nil {
		internalError(w, "load earnings", err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func parseTime(v string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return nil, errors.New("bad time")
	}
	return &t, nil
}
