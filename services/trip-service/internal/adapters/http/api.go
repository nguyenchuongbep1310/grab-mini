// Package http is trip-service's HTTP API: routes, the JSON shapes (which are
// part of the contract with the apps) and the translation to and from the use
// cases. The use cases decide; this package only reads the request, names the
// caller and writes the answer.
package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
)

// API serves the trip routes.
type API struct {
	svc *app.Service
}

func New(svc *app.Service) *API { return &API{svc: svc} }

// Routes is the HTTP API. Every route but /health needs a token and names the
// roles allowed; the use cases then check the caller is a party to the trip.
func (a *API) Routes(verifier *auth.Verifier) *http.ServeMux {
	anyone := func(h http.HandlerFunc) http.Handler { return verifier.Require(h, auth.Rider, auth.Driver) }
	only := func(role auth.Role, h http.HandlerFunc) http.Handler { return verifier.Require(h, role) }
	// The calls that change a trip (and the rating), not the reads the apps
	// poll with.
	writes := auth.LimiterFromEnv(2, 10)
	limited := func(h http.HandlerFunc) http.HandlerFunc { return writes.Wrap(h).ServeHTTP }

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.Handle("GET /trips/{id}", anyone(a.handleGetTrip))
	mux.Handle("GET /trips/by-order/{orderId}", anyone(a.handleGetTripByOrder))
	mux.Handle("GET /drivers/{id}/active-trip", only(auth.Driver, a.handleGetActiveTrip))
	mux.Handle("POST /trips/{id}/pickup", only(auth.Driver, limited(a.handlePickup)))
	mux.Handle("POST /trips/{id}/complete", only(auth.Driver, limited(a.handleComplete)))
	mux.Handle("POST /trips/{id}/cancel", anyone(limited(a.handleCancel)))
	mux.Handle("POST /trips/{id}/receipt/close", only(auth.Rider, limited(a.handleCloseReceipt)))
	mux.Handle("POST /trips/{id}/rating", only(auth.Rider, limited(a.handleRateTrip)))
	return mux
}

// caller returns who is calling, or writes a 401 and reports false.
func caller(w http.ResponseWriter, r *http.Request) (domain.Actor, bool) {
	c, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return domain.Actor{}, false
	}
	who := domain.Actor{ID: c.Subject}
	switch c.Role {
	case auth.Rider:
		who.Role = domain.RoleRider
	case auth.Driver:
		who.Role = domain.RoleDriver
	}
	return who, true
}

// The error bodies: http.Error with a JSON string, as the apps have always received them.
func fail(w http.ResponseWriter, status int, body string) { http.Error(w, body, status) }

func tripNotFound(w http.ResponseWriter) { fail(w, http.StatusNotFound, `{"error":"trip not found"}`) }
func internalError(w http.ResponseWriter) {
	fail(w, http.StatusInternalServerError, `{"error":"internal error"}`)
}

// failure answers the errors every use case can return; it reports false for
// any other error, which the caller maps itself.
func failure(w http.ResponseWriter, err error) bool {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		fail(w, http.StatusBadRequest, `{"error":"`+ve.Msg+`"}`)
	case errors.Is(err, app.ErrTripNotFound):
		tripNotFound(w)
	case errors.Is(err, app.ErrInternal):
		internalError(w)
	default:
		return false
	}
	return true
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// tripJSON is the trip as the apps read it.
type tripJSON struct {
	ID       string `json:"id"`
	OrderID  string `json:"orderId"`
	DriverID string `json:"driverId"`
	RiderID  string `json:"riderId"`
	Status   string `json:"status"`
	// Set once the rider has rated the trip.
	Rating *int `json:"rating,omitempty"`
	// Set once CANCELLED: RIDER or DRIVER, and the reason they gave (if any).
	CancelledBy  string `json:"cancelledBy,omitempty"`
	CancelReason string `json:"cancelReason,omitempty"`
	// When the trip was completed (RFC 3339, from trips.completed_at). Only
	// COMPLETED trips have one; it is omitted for every other status, a
	// cancelled trip included. Written once, by the first completion.
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	// When the rider closed the receipt of the finished ride, or rated it
	// (RFC 3339, from trips.receipt_closed_at). Omitted while still open.
	ReceiptClosedAt *time.Time `json:"receiptClosedAt,omitempty"`
}

func toJSON(t domain.Trip) tripJSON {
	return tripJSON{
		ID: t.ID, OrderID: t.OrderID, DriverID: t.DriverID, RiderID: t.RiderID, Status: string(t.Status),
		Rating: t.Rating, CancelledBy: t.CancelledBy, CancelReason: t.CancelReason,
		CompletedAt: t.CompletedAt, ReceiptClosedAt: t.ReceiptClosedAt,
	}
}

// transitionBody is the response of pickup/complete/cancel: the trip id and
// status, plus completedAt once the trip is COMPLETED.
func transitionBody(t domain.Trip) map[string]string {
	body := map[string]string{"tripId": t.ID, "status": string(t.Status)}
	if t.CompletedAt != nil {
		body["completedAt"] = t.CompletedAt.UTC().Format(time.RFC3339Nano)
	}
	return body
}

// respondWithTrip sends the trip to its rider or its driver. To anyone else it
// is a 404: whether a trip exists is not theirs to learn.
func respondWithTrip(w http.ResponseWriter, t domain.Trip, err error) {
	if err != nil {
		if !failure(w, err) {
			internalError(w)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toJSON(t))
}

func (a *API) handleGetTrip(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	t, err := a.svc.Trip(r.Context(), who, r.PathValue("id"))
	respondWithTrip(w, t, err)
}

func (a *API) handleGetTripByOrder(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	t, err := a.svc.TripByOrder(r.Context(), who, r.PathValue("orderId"))
	respondWithTrip(w, t, err)
}

func (a *API) handleGetActiveTrip(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	t, err := a.svc.ActiveTrip(r.Context(), who, r.PathValue("id"))
	if errors.Is(err, app.ErrForbidden) {
		auth.Deny(w, http.StatusForbidden, err.Error())
		return
	}
	respondWithTrip(w, t, err)
}

func (a *API) handlePickup(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	t, err := a.svc.Pickup(r.Context(), who, r.PathValue("id"))
	respondWithTransition(w, t, err)
}

func (a *API) handleComplete(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	t, err := a.svc.Complete(r.Context(), who, r.PathValue("id"))
	respondWithTransition(w, t, err)
}

// handleCancel reads the optional {"reason": "..."} body; a body that is not
// JSON is refused here, a reason that is too long by the use case.
func (a *API) handleCancel(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		fail(w, http.StatusBadRequest, `{"error":"invalid JSON body"}`)
		return
	}
	t, err := a.svc.Cancel(r.Context(), who, r.PathValue("id"), body.Reason)
	respondWithTransition(w, t, err)
}

func respondWithTransition(w http.ResponseWriter, t domain.Trip, err error) {
	var refusal *domain.Refusal
	var cannot *app.CannotTransitionError
	switch {
	case errors.As(err, &refusal):
		writeRefusal(w, refusal)
		return
	case errors.As(err, &cannot):
		fail(w, http.StatusConflict, `{"error":"trip cannot transition to `+string(cannot.To)+` from its current status"}`)
		return
	case err != nil:
		if !failure(w, err) {
			internalError(w)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(transitionBody(t))
}

// writeRefusal answers a pickup/complete the proximity check refused: 503 when
// the location could not be read (the driver retries), 409 otherwise.
func writeRefusal(w http.ResponseWriter, r *domain.Refusal) {
	status := http.StatusConflict
	if r.Reason == domain.ReasonLocationUnavailable {
		status = http.StatusServiceUnavailable
	}
	body := map[string]any{"error": r.Msg, "code": r.Code}
	if r.Code == "driver_too_far" {
		body["distanceM"], body["maxM"] = int(r.DistanceM+0.5), int(r.MaxM)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

type rateRequest struct {
	RiderID string `json:"riderId"`
	Stars   int    `json:"stars"`
	Comment string `json:"comment"`
	// TipAmount is optional, whole VND (0 to domain.MaxTipAmount). It is recorded only.
	TipAmount int64 `json:"tipAmount"`
}

// A rider rates a completed trip once, from 1 to 5 stars. The rider is whoever
// the token says; a body naming someone else is refused.
func (a *API) handleRateTrip(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}

	var req rateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, `{"error":"invalid JSON body"}`)
		return
	}
	rated, err := a.svc.RateTrip(r.Context(), who, r.PathValue("id"), app.RateInput{
		RiderID: req.RiderID, Stars: req.Stars, Comment: req.Comment, Tip: req.TipAmount,
	})
	switch {
	case errors.Is(err, app.ErrRiderMismatch):
		auth.Deny(w, http.StatusForbidden, err.Error())
		return
	case errors.Is(err, app.ErrNotCompleted):
		fail(w, http.StatusConflict, `{"error":"only completed trips can be rated"}`)
		return
	case errors.Is(err, app.ErrAlreadyRated):
		fail(w, http.StatusConflict, `{"error":"this trip was already rated"}`)
		return
	case err != nil:
		if !failure(w, err) {
			internalError(w)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"tripId": rated.TripID, "stars": rated.Stars, "tipAmount": rated.Tip})
}

// handleCloseReceipt records that the rider closed the finished ride's receipt,
// so no device shows it again. Idempotent: the first time is kept.
func (a *API) handleCloseReceipt(w http.ResponseWriter, r *http.Request) {
	who, ok := caller(w, r)
	if !ok {
		return
	}
	closed, err := a.svc.CloseReceipt(r.Context(), who, r.PathValue("id"))
	switch {
	case errors.Is(err, app.ErrNotCompleted):
		fail(w, http.StatusConflict, `{"error":"not_completed"}`)
		return
	case err != nil:
		if !failure(w, err) {
			internalError(w)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"tripId": closed.TripID, "receiptClosedAt": closed.At.Format(time.RFC3339Nano)})
}
