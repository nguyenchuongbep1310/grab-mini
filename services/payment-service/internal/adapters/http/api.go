// Package http is payment-service's HTTP API: routes, the JSON shapes (which are
// part of the contract with the apps) and the translation to and from the use cases.
package http

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"grab-mini/payment-service/internal/app"
	"grab-mini/payment-service/internal/domain"
	"grab-mini/pkg/auth"
)

// API serves the three read routes.
type API struct {
	svc *app.Service
}

func New(svc *app.Service) *API { return &API{svc: svc} }

// Routes is the HTTP API. Both routes need a token; the handlers then check
// the caller is the rider or the driver the money belongs to.
func (a *API) Routes(verifier *auth.Verifier) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /payments", verifier.Require(http.HandlerFunc(a.GetPayments), auth.Rider, auth.Driver))
	mux.Handle("GET /cancellation-policy", verifier.Require(http.HandlerFunc(a.CancellationPolicy), auth.Rider, auth.Driver))
	mux.Handle("GET /drivers/{id}/earnings", verifier.Require(http.HandlerFunc(a.Earnings), auth.Driver))
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

// CancellationPolicy serves GET /cancellation-policy: what a rider pays for cancelling
// after a driver was assigned, so the app can say so before the rider taps "cancel". The fee
// applies only when the rider cancels more than graceSeconds after the assignment; 0 means
// cancelling is free.
func (a *API) CancellationPolicy(w http.ResponseWriter, r *http.Request) {
	if _, ok := auth.From(r.Context()); !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	p := a.svc.CancellationPolicy()
	writeJSON(w, http.StatusOK, map[string]any{
		"currency":     p.Currency,
		"fee":          p.Fee,
		"graceSeconds": p.GraceSeconds,
	})
}

// GetPayments serves GET /payments?tripId=. A rider sees the charges of
// their own trip, a driver the payouts of their own: for anyone else the list
// is empty, exactly as for a trip with no payments yet, so the endpoint does
// not reveal whether a trip exists. total sums the captured amounts.
func (a *API) GetPayments(w http.ResponseWriter, r *http.Request) {
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
		list, total, err := a.svc.DriverPayouts(r.Context(), tripID, id.Subject)
		if err != nil {
			internalError(w, "load payouts", err)
			return
		}
		payouts := make([]driverPayout, 0, len(list))
		for _, p := range list {
			payouts = append(payouts, driverPayout(p))
		}
		writeJSON(w, http.StatusOK, map[string]any{"tripId": tripID, "currency": domain.CurrencyVND, "payouts": payouts, "total": total})
		return
	}
	list, total, err := a.svc.RiderPayments(r.Context(), tripID, id.Subject)
	if err != nil {
		internalError(w, "load payments", err)
		return
	}
	payments := make([]riderPayment, 0, len(list))
	for _, p := range list {
		payments = append(payments, riderPayment(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"tripId": tripID, "currency": domain.CurrencyVND, "payments": payments, "total": total})
}

// Earnings serves GET /drivers/{id}/earnings[?from=&to=] (RFC 3339; from
// inclusive, to exclusive). A driver may read only their own.
func (a *API) Earnings(w http.ResponseWriter, r *http.Request) {
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
	e, err := a.svc.DriverEarnings(r.Context(), id.Subject, from, to)
	if err != nil {
		internalError(w, "load earnings", err)
		return
	}
	writeJSON(w, http.StatusOK, earnings(e))
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
