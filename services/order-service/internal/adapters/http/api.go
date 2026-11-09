// Package http is order-service's HTTP API: routes, the JSON shapes (part of
// the contract with the apps) and the translation to and from the use cases.
// The use cases decide; this package reads the request, names the caller and
// writes the answer, byte for byte as the service always has.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/surge"
)

// service is the part of *app.Service the API calls.
type service interface {
	CreateOrder(ctx context.Context, cmd app.CreateOrderCommand) (app.CreateOrderResult, error)
	CancelOrder(ctx context.Context, actor domain.Actor, orderID, reason string) (app.CancelResult, error)
	GetOrder(ctx context.Context, actor domain.Actor, id string) (domain.Order, error)
	LatestOrder(ctx context.Context, actor domain.Actor, riderID string) (domain.Order, error)
}

var _ service = (*app.Service)(nil)

// API serves the order routes.
type API struct {
	svc      service
	verifier *auth.Verifier
}

func NewAPI(svc *app.Service, verifier *auth.Verifier) *API { return newAPI(svc, verifier) }

func newAPI(svc service, verifier *auth.Verifier) *API { return &API{svc: svc, verifier: verifier} }

// Routes is the HTTP API. Every route but /health needs a token, and says
// which roles may call it; the use cases then check the caller owns what they
// touch.
func (a *API) Routes() *http.ServeMux {
	// Placing and cancelling orders is what a person does a few times; a
	// script that does it faster is abusing the pricing, matching and database
	// behind it.
	writes := auth.LimiterFromEnv(1, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.Handle("POST /orders", a.verifier.Require(writes.Wrap(http.HandlerFunc(a.handleCreateOrder)), auth.Rider))
	mux.Handle("GET /orders/{id}", a.verifier.Require(http.HandlerFunc(a.handleGetOrder), auth.Rider, auth.Driver))
	mux.Handle("POST /orders/{id}/cancel", a.verifier.Require(writes.Wrap(http.HandlerFunc(a.handleCancelOrder)), auth.Rider))
	mux.Handle("GET /riders/{id}/latest-order", a.verifier.Require(http.HandlerFunc(a.handleGetLatestOrder), auth.Rider))
	return mux
}

// actor names the caller for the use cases.
func actor(c auth.Identity) domain.Actor {
	a := domain.Actor{Subject: c.Subject}
	switch c.Role {
	case auth.Rider:
		a.Role = domain.RoleRider
	case auth.Driver:
		a.Role = domain.RoleDriver
	}
	return a
}

// fail writes an error body as the apps have always received it: http.Error
// with a JSON string.
func fail(w http.ResponseWriter, status int, body string) { http.Error(w, body, status) }

func internalError(w http.ResponseWriter) {
	fail(w, http.StatusInternalServerError, `{"error":"internal error"}`)
}

func orderNotFound(w http.ResponseWriter) {
	fail(w, http.StatusNotFound, `{"error":"order not found"}`)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type latLngJSON struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type createOrderRequest struct {
	RiderID string      `json:"riderId"`
	Pickup  *latLngJSON `json:"pickup"`
	Dropoff *latLngJSON `json:"dropoff"`
	// Optional; defaults to CAR.
	VehicleType string `json:"vehicleType"`
	// Optional: the quote the rider was shown (GET /quotes on Pricing Service).
	QuoteID string `json:"quoteId"`
	// Optional: the promoToken from GET /promotions/validate (Promotion Service).
	PromoToken string `json:"promoToken"`
}

func point(p *latLngJSON) *domain.LatLng {
	if p == nil {
		return nil
	}
	return &domain.LatLng{Lat: p.Lat, Lng: p.Lng}
}

// handleCreateOrder is US-1.1: create an order (the use case publishes order.created).
func (a *API) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}

	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, `{"error":"invalid JSON body"}`)
		return
	}

	// The rider is whoever the token says. A body that names someone else is
	// refused rather than quietly overridden: it is a bug or an attack.
	if req.RiderID != "" && req.RiderID != caller.Subject {
		auth.Deny(w, http.StatusForbidden, "riderId doesn't match the token")
		return
	}

	key, err := domain.ParseIdempotencyKey(r.Header.Values("Idempotency-Key"))
	if err != nil {
		fail(w, http.StatusBadRequest, `{"error":"`+err.Error()+`"}`)
		return
	}

	booking, err := domain.BookingRequest{
		Pickup: point(req.Pickup), Dropoff: point(req.Dropoff),
		VehicleType: domain.VehicleType(req.VehicleType),
		QuoteID:     req.QuoteID, PromoToken: req.PromoToken,
	}.Normalize()
	switch {
	case errors.Is(err, domain.ErrBadCoordinates):
		fail(w, http.StatusBadRequest, `{"error":"pickup {lat,lng} and dropoff {lat,lng} are required"}`)
		return
	case errors.Is(err, domain.ErrBadVehicleType):
		fail(w, http.StatusBadRequest, `{"error":"vehicleType must be CAR or BIKE"}`)
		return
	case err != nil:
		internalError(w)
		return
	}

	res, err := a.svc.CreateOrder(r.Context(), app.CreateOrderCommand{RiderID: caller.Subject, IdempotencyKey: key, Request: booking})
	if err != nil {
		writeBookingError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.WriteHeader(res.Status)
	w.Write(res.Body)
}

// writeBookingError maps what CreateOrder can refuse to its response.
func writeBookingError(w http.ResponseWriter, err error) {
	var moved *app.QuoteConflict
	var refused *app.PromoError
	switch {
	case errors.As(err, &moved):
		writeJSON(w, http.StatusConflict, map[string]any{"error": moved.Error(), "newQuote": moved.NewQuote})
	case errors.Is(err, app.ErrInvalidQuote):
		fail(w, http.StatusBadRequest, `{"error":"quote is invalid: request a new one"}`)
	case errors.As(err, &refused):
		status := http.StatusBadRequest
		if refused.Conflict {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": refused.Message, "code": refused.Code})
	case errors.Is(err, app.ErrKeyMismatch):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(`{"error":"this Idempotency-Key was already used with a different request body"}` + "\n"))
	default:
		internalError(w)
	}
}

// orderJSON is what clients see of an order. The driver it was matched to is
// never sent.
type orderJSON struct {
	ID          string     `json:"id"`
	RiderID     string     `json:"riderId"`
	Pickup      latLngJSON `json:"pickup"`
	Dropoff     latLngJSON `json:"dropoff"`
	VehicleType string     `json:"vehicleType"`
	// PENDING until matching reports back, then MATCHED or NO_DRIVER.
	Status string `json:"status"`
	// Set once CANCELLED: when, by whom (RIDER or DRIVER) and why.
	CancelledAt  *time.Time `json:"cancelledAt,omitempty"`
	CancelledBy  string     `json:"cancelledBy,omitempty"`
	CancelReason string     `json:"cancelReason,omitempty"`
	// What the rider pays (VND) and the surge multiplier it includes; Price is
	// null on orders booked before surge pricing existed.
	SurgeMultiplier float64  `json:"surgeMultiplier"`
	Price           *float64 `json:"price"`
	// The promo code the order booked with and the VND it takes off the price;
	// FinalPrice is what the rider pays, null with Price.
	PromoCode  string   `json:"promoCode,omitempty"`
	Discount   int64    `json:"discount"`
	FinalPrice *float64 `json:"finalPrice"`
}

func toJSON(o domain.Order) orderJSON {
	return orderJSON{
		ID: o.ID, RiderID: o.RiderID,
		Pickup:      latLngJSON{o.Pickup.Lat, o.Pickup.Lng},
		Dropoff:     latLngJSON{o.Dropoff.Lat, o.Dropoff.Lng},
		VehicleType: string(o.VehicleType), Status: string(o.Status),
		CancelledAt: o.CancelledAt, CancelledBy: o.CancelledBy, CancelReason: o.CancelReason,
		SurgeMultiplier: o.SurgeMultiplier, Price: o.Price,
		PromoCode: o.PromoCode, Discount: o.Discount, FinalPrice: o.FinalPrice(),
	}
}

// respondWithOrder sends the order, or maps a failure of the lookup.
func respondWithOrder(w http.ResponseWriter, o domain.Order, err error) {
	switch {
	case errors.Is(err, domain.ErrOrderNotFound):
		orderNotFound(w)
	case errors.Is(err, app.ErrForbidden):
		auth.Deny(w, http.StatusForbidden, err.Error())
	case err != nil:
		internalError(w)
	default:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toJSON(o))
	}
}

// handleGetOrder lets a client that only knows the orderId (e.g. the Driver
// App after an order.matched push) find where the trip starts and ends.
func (a *API) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	o, err := a.svc.GetOrder(r.Context(), actor(caller), r.PathValue("id"))
	respondWithOrder(w, o, err)
}

// handleGetLatestOrder is how a Rider App that was closed or reloaded finds
// the ride it may still be in the middle of.
func (a *API) handleGetLatestOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusForbidden, app.ErrForbidden.Error())
		return
	}
	o, err := a.svc.LatestOrder(r.Context(), actor(caller), r.PathValue("id"))
	respondWithOrder(w, o, err)
}

type cancelOrderRequest struct {
	Reason string `json:"reason"`
}

// handleCancelOrder lets a rider cancel an order that has no driver yet; the
// use case says what each status means.
func (a *API) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	id := r.PathValue("id")
	// orders.id is a UUID column: a malformed id is "not found", not an error.
	if _, err := uuid.Parse(id); err != nil {
		orderNotFound(w)
		return
	}

	var req cancelOrderRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		fail(w, http.StatusBadRequest, `{"error":"invalid JSON body"}`)
		return
	}
	reason, err := domain.NormalizeCancelReason(req.Reason)
	if err != nil {
		fail(w, http.StatusBadRequest, `{"error":"reason is at most 200 characters"}`)
		return
	}

	res, err := a.svc.CancelOrder(r.Context(), actor(caller), id, reason)
	switch {
	case errors.Is(err, domain.ErrOrderNotFound):
		orderNotFound(w)
	case err != nil:
		internalError(w)
	case res.Conflict:
		writeJSON(w, http.StatusConflict, map[string]string{"error": res.Message, "status": string(res.Order.Status)})
	default:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toJSON(res.Order))
	}
}

// ReceiptEncoder renders the response to a new booking. The bytes are stored
// with an Idempotency-Key and replayed as they are, so the encoding must not
// change: keys sorted, promoCode only when there is one, a trailing newline.
func ReceiptEncoder() app.ReceiptEncoder {
	return func(r app.Receipt) []byte {
		resp := map[string]any{
			"orderId": r.OrderID, "status": string(domain.StatusPending),
			"surgeMultiplier": r.SurgeMultiplier, "price": r.Price, "currency": surge.Currency,
			"discount": r.Discount, "finalPrice": r.FinalPrice,
		}
		if r.PromoCode != "" {
			resp["promoCode"] = r.PromoCode
		}
		body, _ := json.Marshal(resp)
		return append(body, '\n')
	}
}
