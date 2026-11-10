// Package http is profile-service's HTTP API: routes, the JSON shapes (part of the contract
// with the apps) and the translation to and from the use cases. The use cases decide; this
// package names the caller, applies the own-profile rule, reads the request and writes the
// answer, byte for byte as the service always has.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	"grab-mini/pkg/auth"
	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
)

// A profile is a few hundred bytes of JSON; the rest is room.
const maxBodyBytes = 8 << 10

// Refusals counts refused requests by code (profile_requests_rejected_total). Only the
// HTTP layer refuses, so only it counts; an internal error is not a refusal.
type Refusals interface{ Refused(code string) }

// API serves the profile routes.
type API struct {
	svc      *app.Service
	verifier *auth.Verifier
	// limiter limits GET /drivers/{id}/public per caller, in memory: with several
	// instances a caller's real allowance is the limit times the instances. nil lets
	// everything through.
	limiter  *auth.Limiter
	refusals Refusals
}

// NewAPI returns the API over svc. limiter may be nil (no limit); refusals may not.
func NewAPI(svc *app.Service, verifier *auth.Verifier, limiter *auth.Limiter, refusals Refusals) *API {
	return &API{svc: svc, verifier: verifier, limiter: limiter, refusals: refusals}
}

// NewPublicLimiter allows a burst of a quarter of a minute's allowance and then perMinute
// lookups a minute. RATE_LIMIT_SCALE scales it, 0 turns it off (load tests), as for every
// other service.
func NewPublicLimiter(perMinute int) *auth.Limiter {
	return auth.LimiterFromEnv(float64(perMinute)/60, max(1, perMinute/4))
}

// Routes is the HTTP API. The person is always the token's subject: a rider or driver can
// read and write only their own profile (a different id in the path is a 403, whatever the
// id), the public view is for any signed-in user, and the review calls are for admins.
func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	rider := func(h http.HandlerFunc) http.Handler { return a.verifier.Require(h, auth.Rider) }
	driver := func(h http.HandlerFunc) http.Handler { return a.verifier.Require(h, auth.Driver) }
	admin := func(h http.HandlerFunc) http.Handler { return a.verifier.Require(h, auth.Admin) }

	mux.Handle("GET /riders/{id}", rider(a.handleGetRider))
	mux.Handle("PUT /riders/{id}", rider(a.handlePutRider))
	mux.Handle("GET /drivers/{id}", driver(a.handleGetDriver))
	mux.Handle("PUT /drivers/{id}", driver(a.handlePutDriver))
	mux.Handle("POST /drivers/{id}/application", driver(a.handleApply))
	mux.Handle("GET /drivers/{id}/public", a.verifier.Require(http.HandlerFunc(a.handlePublic))) // any signed-in user
	mux.Handle("GET /driver-applications", admin(a.handleList))
	mux.Handle("POST /driver-applications/resync", admin(a.handleResync))
	for _, action := range domain.ActionNames() {
		mux.Handle("POST /drivers/{id}/"+action, admin(a.handleReview(action)))
	}
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	// Profiles are personal data: no browser or proxy cache keeps them.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// refuse answers a refused request with a message for people and a stable code for
// programs, and counts it. extra adds fields to the body ("field", "status").
func (a *API) refuse(w http.ResponseWriter, status int, code, msg string, extra ...string) {
	a.refusals.Refused(code)
	body := map[string]string{"error": msg, "code": code}
	for i := 0; i+1 < len(extra); i += 2 {
		body[extra[i]] = extra[i+1]
	}
	writeJSON(w, status, body)
}

func internalError(w http.ResponseWriter, what string, err error) {
	// err can carry a driver id or a SQL state; the rows themselves are never in it.
	log.Printf("%s: %v", what, err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error", "code": "internal"})
}

func (a *API) invalid(w http.ResponseWriter, err *domain.FieldError) {
	a.refuse(w, http.StatusBadRequest, "invalid_profile", err.Error(), "field", err.Field)
}

func (a *API) notFound(w http.ResponseWriter) {
	a.refuse(w, http.StatusNotFound, "not_found", "no such driver")
}

// own checks that the path names the caller. Anyone else's id is a 403 whether or not
// that profile exists, so the answer does not say who has one.
func (a *API) own(w http.ResponseWriter, r *http.Request) (auth.Identity, bool) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return caller, false
	}
	if r.PathValue("id") != caller.Subject {
		a.refuse(w, http.StatusForbidden, "forbidden", "you can only use your own profile")
		return caller, false
	}
	return caller, true
}

// readBody decodes a JSON object. Unknown fields are ignored on purpose: a client may send
// back what it read (status, updatedAt...) and those are never taken from the body.
// An empty body is an error unless optional.
func (a *API) readBody(w http.ResponseWriter, r *http.Request, v any, optional bool) bool {
	err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(v)
	if errors.Is(err, io.EOF) && optional {
		return true
	}
	if err != nil {
		a.refuse(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return false
	}
	return true
}

// fieldError reports whether err is a field the caller got wrong, and answers it.
func (a *API) fieldError(w http.ResponseWriter, err error) bool {
	var ferr *domain.FieldError
	if errors.As(err, &ferr) {
		a.invalid(w, ferr)
		return true
	}
	return false
}

// ---------------------------------------------------------------- riders ----

// handleGetRider serves GET /riders/{id}: a rider without a profile yet gets an empty one
// (200), so an app does not have to treat the first visit as an error.
func (a *API) handleGetRider(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.own(w, r)
	if !ok {
		return
	}
	rider, err := a.svc.GetRider(r.Context(), caller.Subject)
	if err != nil {
		internalError(w, "load rider "+caller.Subject, err)
		return
	}
	writeJSON(w, http.StatusOK, newRiderView(rider))
}

type riderRequest struct {
	Name  string `json:"name"`
	Phone string `json:"phone"`
}

// handlePutRider serves PUT /riders/{id}: the name (required) and the phone (optional)
// replace what was there.
func (a *API) handlePutRider(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.own(w, r)
	if !ok {
		return
	}
	var req riderRequest
	if !a.readBody(w, r, &req, false) {
		return
	}
	rider, err := a.svc.SaveRider(r.Context(), app.SaveRiderCommand{ID: caller.Subject, Name: req.Name, Phone: req.Phone})
	if a.fieldError(w, err) {
		return
	}
	if err != nil {
		internalError(w, "save rider "+caller.Subject, err)
		return
	}
	writeJSON(w, http.StatusOK, newRiderView(rider))
}

// --------------------------------------------------------------- drivers ----

// handleGetDriver serves GET /drivers/{id}: the driver's own profile and status. A driver
// who never saved one gets status NONE (200).
func (a *API) handleGetDriver(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.own(w, r)
	if !ok {
		return
	}
	d, err := a.svc.GetDriver(r.Context(), caller.Subject)
	if err != nil {
		internalError(w, "load driver "+caller.Subject, err)
		return
	}
	writeJSON(w, http.StatusOK, newDriverView(d))
}

type driverRequest struct {
	Name        string `json:"name"`
	Phone       string `json:"phone"`
	VehicleType string `json:"vehicleType"`
	Plate       string `json:"plate"`
	Model       string `json:"model"`
	Color       string `json:"color"`
}

// handlePutDriver serves PUT /drivers/{id}: replaces name, phone, vehicle type, plate, model
// and colour (the status is never taken from the body). Changing the plate or the vehicle
// type of an APPROVED driver puts them back under review and out of service until an admin
// approves the new vehicle (see app.Service.SaveDriver): the response then has status PENDING.
func (a *API) handlePutDriver(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.own(w, r)
	if !ok {
		return
	}
	var req driverRequest
	if !a.readBody(w, r, &req, false) {
		return
	}
	saved, err := a.svc.SaveDriver(r.Context(), app.SaveDriverCommand{
		ID: caller.Subject, Name: req.Name, Phone: req.Phone, VehicleType: req.VehicleType,
		Plate: req.Plate, Model: req.Model, Color: req.Color,
	})
	if a.fieldError(w, err) {
		return
	}
	var incomplete *domain.IncompleteError
	switch {
	case errors.As(err, &incomplete):
		a.refuse(w, http.StatusBadRequest, "invalid_profile", "an application under review needs: "+strings.Join(incomplete.Missing, ", "), "field", incomplete.Missing[0])
		return
	case err != nil:
		internalError(w, "save driver "+caller.Subject, err)
		return
	}
	writeJSON(w, http.StatusOK, newDriverView(saved.Driver))
}

// handleApply serves POST /drivers/{id}/application: submit the saved profile for review.
// 201 when the driver is now PENDING; 200 with no change when they already were PENDING or
// APPROVED (a retry is safe); 409 suspended when an admin suspended them; 422 when the
// profile lacks a name, vehicle type or plate.
func (a *API) handleApply(w http.ResponseWriter, r *http.Request) {
	caller, ok := a.own(w, r)
	if !ok {
		return
	}
	res, err := a.svc.Apply(r.Context(), caller.Subject)
	var incomplete *domain.IncompleteError
	switch {
	case errors.Is(err, domain.ErrNoProfile):
		a.refuse(w, http.StatusUnprocessableEntity, "incomplete_profile", "save your profile (name, vehicle type, plate) before applying", "field", "name")
		return
	case errors.As(err, &incomplete):
		a.refuse(w, http.StatusUnprocessableEntity, "incomplete_profile", "fill in before applying: "+strings.Join(incomplete.Missing, ", "), "field", incomplete.Missing[0])
		return
	case errors.Is(err, domain.ErrSuspended):
		a.refuse(w, http.StatusConflict, "suspended", "your account is suspended; contact support", "status", domain.StatusSuspended)
		return
	case err != nil:
		internalError(w, "apply driver "+caller.Subject, err)
		return
	}
	if !res.Submitted {
		writeJSON(w, http.StatusOK, newDriverView(res.Driver))
		return
	}
	writeJSON(w, http.StatusCreated, newDriverView(res.Driver))
}

// handlePublic serves GET /drivers/{id}/public: the name, vehicle and plate of an APPROVED
// driver, for the rider whose trip they are on. Anything else (no such driver, not approved,
// suspended) is the same 404, so the endpoint does not say who has applied. Never the phone.
// Limited per caller, because ids are easy to guess.
func (a *API) handlePublic(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	if a.limiter != nil {
		if allowed, wait := a.limiter.Allow(string(caller.Role) + ":" + caller.Subject); !allowed {
			seconds := max(1, int(math.Ceil(wait.Seconds())))
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			a.refusals.Refused("rate_limited")
			// Also in the body, which a browser app can read without CORS exposing the header.
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "too many lookups, slow down", "code": "rate_limited", "retryAfter": seconds})
			return
		}
	}
	d, err := a.svc.PublicDriver(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, domain.ErrNoDriver):
		a.notFound(w)
		return
	case err != nil:
		internalError(w, "load public driver", err)
		return
	}
	writeJSON(w, http.StatusOK, newPublicDriverView(d))
}

// --------------------------------------------------------------- reviews ----

// handleList serves GET /driver-applications?status=PENDING&limit=&cursor=: the drivers with
// one status, the ones waiting longest first (admin only; includes phone numbers, because a
// reviewer may need to call). status defaults to PENDING. cursor is the nextCursor of the
// previous page, so pages neither skip nor repeat a driver while others are being decided.
func (a *API) handleList(w http.ResponseWriter, r *http.Request) {
	page, err := a.svc.ListApplications(r.Context(), listQuery(r))
	var bad *domain.InvalidRequestError
	switch {
	case errors.As(err, &bad):
		a.refuse(w, http.StatusBadRequest, "invalid_request", bad.Message)
		return
	case err != nil:
		internalError(w, "list driver applications", err)
		return
	}
	views := make([]adminDriverView, len(page.Drivers))
	for i, d := range page.Drivers {
		views[i] = newAdminDriverView(d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"applications": views, "nextCursor": page.NextCursor})
}

type reviewRequest struct {
	Reason string `json:"reason"`
}

// handleReview serves POST /drivers/{id}/approve|reject|suspend (admin). reject and suspend
// need a reason of 1 to 500 characters, which the driver sees; approve may have one. The
// driver's status must allow the action, or it is a 409 naming the status they have. The
// decision and its driver.application.reviewed commit together.
func (a *API) handleReview(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := auth.From(r.Context())
		if !ok {
			auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		var req reviewRequest
		if !a.readBody(w, r, &req, true) {
			return
		}
		id := r.PathValue("id")
		d, err := a.svc.Review(r.Context(), app.ReviewCommand{DriverID: id, Action: action, AdminID: caller.Subject, Reason: req.Reason})
		if a.fieldError(w, err) {
			return
		}
		var te *domain.TransitionError
		switch {
		case errors.Is(err, domain.ErrNoDriver):
			a.notFound(w)
			return
		case errors.As(err, &te):
			a.refuse(w, http.StatusConflict, "invalid_transition", te.Error(), "status", te.From)
			return
		case err != nil:
			internalError(w, action+" driver "+id, err)
			return
		}
		writeJSON(w, http.StatusOK, newAdminDriverView(d))
	}
}

type resyncRequest struct {
	DriverID string `json:"driverId"`
}

// handleResync serves POST /driver-applications/resync (admin): re-publishes the current
// status of every APPROVED, REJECTED or SUSPENDED driver as driver.application.reviewed, or of
// the one in the optional body {"driverId": "..."}, so Matching can rebuild what a Redis that
// lost its data forgot (see app.Service.Resync). 200 with the counts per status; 404 for an
// unknown driverId; 409 resync_running while another resync runs. The run finishes even if
// the caller goes away. For a very large fleet the response can outlast the server's write
// timeout: the work is done all the same, and `profile-service resync` has no such limit.
func (a *API) handleResync(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	var req resyncRequest
	if !a.readBody(w, r, &req, true) {
		return
	}
	if req.DriverID != "" && !domain.ValidDriverID(req.DriverID) {
		a.notFound(w)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), app.ResyncTimeout)
	defer cancel()
	res, err := a.svc.Resync(ctx, req.DriverID)
	switch {
	case errors.Is(err, app.ErrResyncRunning):
		a.refuse(w, http.StatusConflict, "resync_running", "a resync is already running; try again when it has finished")
		return
	case errors.Is(err, domain.ErrNoDriver):
		a.notFound(w)
		return
	case err != nil:
		internalError(w, "resync", err)
		return
	}
	scope := "all drivers"
	if req.DriverID != "" {
		scope = "driver " + req.DriverID
	}
	log.Printf("resync by %s: %s: %d event(s) queued (APPROVED %d, REJECTED %d, SUSPENDED %d, %d skipped)", caller.Subject, scope,
		res.Total, res.Counts[domain.StatusApproved], res.Counts[domain.StatusRejected], res.Counts[domain.StatusSuspended], res.Skipped)
	writeJSON(w, http.StatusOK, map[string]any{"queued": res.Total, "counts": res.Counts, "skipped": res.Skipped})
}
