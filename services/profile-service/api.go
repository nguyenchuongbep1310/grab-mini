package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"grab-mini/pkg/auth"
)

const (
	// A profile is a few hundred bytes of JSON; the rest is room.
	maxBodyBytes = 8 << 10

	defaultPageSize = 50
	maxPageSize     = 100
	maxIDLen        = 128
)

// routes is the HTTP API. The person is always the token's subject: a rider or driver
// can read and write only their own profile (a different id in the path is a 403, whatever
// the id), the public view is for any signed-in user, and the review calls are for admins.
func (s *service) routes(verifier *auth.Verifier) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	rider := func(h http.HandlerFunc) http.Handler { return verifier.Require(h, auth.Rider) }
	driver := func(h http.HandlerFunc) http.Handler { return verifier.Require(h, auth.Driver) }
	admin := func(h http.HandlerFunc) http.Handler { return verifier.Require(h, auth.Admin) }

	mux.Handle("GET /riders/{id}", rider(s.handleGetRider))
	mux.Handle("PUT /riders/{id}", rider(s.handlePutRider))
	mux.Handle("GET /drivers/{id}", driver(s.handleGetDriver))
	mux.Handle("PUT /drivers/{id}", driver(s.handlePutDriver))
	mux.Handle("POST /drivers/{id}/application", driver(s.handleApply))
	mux.Handle("GET /drivers/{id}/public", verifier.Require(http.HandlerFunc(s.handlePublic))) // any signed-in user
	mux.Handle("GET /driver-applications", admin(s.handleList))
	mux.Handle("POST /driver-applications/resync", admin(s.handleResync))
	for action := range reviewActions {
		mux.Handle("POST /drivers/{id}/"+action, admin(s.handleReview(action)))
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
func refuse(w http.ResponseWriter, status int, code, msg string, extra ...string) {
	requestsRefused.WithLabelValues(code).Inc()
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

func invalid(w http.ResponseWriter, err *fieldError) {
	refuse(w, http.StatusBadRequest, "invalid_profile", err.Error(), "field", err.field)
}

// own checks that the path names the caller. Anyone else's id is a 403 whether or not
// that profile exists, so the answer does not say who has one.
func own(w http.ResponseWriter, r *http.Request) (auth.Identity, bool) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return caller, false
	}
	if r.PathValue("id") != caller.Subject {
		refuse(w, http.StatusForbidden, "forbidden", "you can only use your own profile")
		return caller, false
	}
	return caller, true
}

// readBody decodes a JSON object. Unknown fields are ignored on purpose: a client may send
// back what it read (status, updatedAt...) and those are never taken from the body.
// An empty body is an error unless optional.
func readBody(w http.ResponseWriter, r *http.Request, v any, optional bool) bool {
	err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(v)
	if errors.Is(err, io.EOF) && optional {
		return true
	}
	if err != nil {
		refuse(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return false
	}
	return true
}

// ---------------------------------------------------------------- riders ----

type riderView struct {
	RiderID   string     `json:"riderId"`
	Name      string     `json:"name"`
	Phone     string     `json:"phone"`
	UpdatedAt *time.Time `json:"updatedAt"`
}

func newRiderView(r riderRow) riderView {
	v := riderView{RiderID: r.ID, Name: r.Name, Phone: r.Phone}
	if !r.UpdatedAt.IsZero() {
		v.UpdatedAt = &r.UpdatedAt
	}
	return v
}

// handleGetRider serves GET /riders/{id}: a rider without a profile yet gets an empty one
// (200), so an app does not have to treat the first visit as an error.
func (s *service) handleGetRider(w http.ResponseWriter, r *http.Request) {
	caller, ok := own(w, r)
	if !ok {
		return
	}
	row, err := s.loadRider(r.Context(), caller.Subject)
	if err != nil && !errors.Is(err, errNoRider) {
		internalError(w, "load rider "+caller.Subject, err)
		return
	}
	writeJSON(w, http.StatusOK, newRiderView(row))
}

type riderRequest struct {
	Name  string `json:"name"`
	Phone string `json:"phone"`
}

// handlePutRider serves PUT /riders/{id}: the name (required) and the phone (optional)
// replace what was there.
func (s *service) handlePutRider(w http.ResponseWriter, r *http.Request) {
	caller, ok := own(w, r)
	if !ok {
		return
	}
	var req riderRequest
	if !readBody(w, r, &req, false) {
		return
	}
	name, ferr := cleanName(req.Name)
	if ferr != nil {
		invalid(w, ferr)
		return
	}
	phone, ferr := cleanPhone(req.Phone)
	if ferr != nil {
		invalid(w, ferr)
		return
	}
	row, err := s.saveRider(r.Context(), caller.Subject, name, phone)
	if err != nil {
		internalError(w, "save rider "+caller.Subject, err)
		return
	}
	profileUpdates.WithLabelValues("rider").Inc()
	log.Printf("rider %s updated their profile", caller.Subject) // ids only
	writeJSON(w, http.StatusOK, newRiderView(row))
}

// --------------------------------------------------------------- drivers ----

// driverView is a driver's own profile. ReviewedBy (an admin's id) is not shown to the driver.
type driverView struct {
	DriverID    string     `json:"driverId"`
	Name        string     `json:"name"`
	Phone       string     `json:"phone"`
	VehicleType string     `json:"vehicleType"`
	Plate       string     `json:"plate"`
	Model       string     `json:"model"`
	Color       string     `json:"color"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason"`
	ReviewedAt  *time.Time `json:"reviewedAt"`
	StatusAt    *time.Time `json:"statusAt"`
	UpdatedAt   *time.Time `json:"updatedAt"`
}

func newDriverView(d driverRow) driverView {
	v := driverView{
		DriverID: d.ID, Name: d.Name, Phone: d.Phone, VehicleType: string(d.Vehicle), Plate: d.Plate,
		Model: d.Model, Color: d.Color, Status: d.Status, Reason: d.Reason, ReviewedAt: d.ReviewedAt,
	}
	if d.Status == "" {
		v.Status = statusNone
	}
	if !d.UpdatedAt.IsZero() {
		v.StatusAt, v.UpdatedAt = &d.StatusAt, &d.UpdatedAt
	}
	return v
}

// adminDriverView is what a reviewer sees: the driver's profile and who decided.
type adminDriverView struct {
	driverView
	ReviewedBy string `json:"reviewedBy"`
}

func newAdminDriverView(d driverRow) adminDriverView {
	return adminDriverView{driverView: newDriverView(d), ReviewedBy: d.ReviewedBy}
}

// publicDriverView is all anyone but the driver and an admin may see: who is driving and
// what they drive. No phone, no status, no history.
type publicDriverView struct {
	DriverID    string `json:"driverId"`
	Name        string `json:"name"`
	VehicleType string `json:"vehicleType"`
	Plate       string `json:"plate"`
	Model       string `json:"model"`
	Color       string `json:"color"`
}

// handleGetDriver serves GET /drivers/{id}: the driver's own profile and status. A driver
// who never saved one gets status NONE (200).
func (s *service) handleGetDriver(w http.ResponseWriter, r *http.Request) {
	caller, ok := own(w, r)
	if !ok {
		return
	}
	d, err := loadDriver(r.Context(), s.db, caller.Subject, false)
	if err != nil && !errors.Is(err, errNoDriver) {
		internalError(w, "load driver "+caller.Subject, err)
		return
	}
	if errors.Is(err, errNoDriver) {
		d = driverRow{ID: caller.Subject, Status: statusNone}
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

func (req driverRequest) clean() (driverInput, *fieldError) {
	var in driverInput
	var err *fieldError
	if in.Name, err = cleanName(req.Name); err != nil {
		return in, err
	}
	if in.Phone, err = cleanPhone(req.Phone); err != nil {
		return in, err
	}
	if in.Vehicle, err = cleanVehicle(req.VehicleType); err != nil {
		return in, err
	}
	if in.Plate, err = cleanPlate(req.Plate); err != nil {
		return in, err
	}
	if in.Model, err = clean("model", req.Model, maxModelRunes); err != nil {
		return in, err
	}
	if in.Color, err = clean("color", req.Color, maxColorRunes); err != nil {
		return in, err
	}
	return in, nil
}

// handlePutDriver serves PUT /drivers/{id}: replaces name, phone, vehicle type, plate, model
// and colour (the status is never taken from the body). Changing the plate or the vehicle
// type of an APPROVED driver puts them back under review and out of service until an admin
// approves the new vehicle (see saveDriver): the response then has status PENDING.
func (s *service) handlePutDriver(w http.ResponseWriter, r *http.Request) {
	caller, ok := own(w, r)
	if !ok {
		return
	}
	var req driverRequest
	if !readBody(w, r, &req, false) {
		return
	}
	in, ferr := req.clean()
	if ferr != nil {
		invalid(w, ferr)
		return
	}
	d, vehicleChanged, err := s.saveDriver(r.Context(), caller.Subject, in)
	var incomplete *incompleteError
	switch {
	case errors.As(err, &incomplete):
		refuse(w, http.StatusBadRequest, "invalid_profile", "an application under review needs: "+strings.Join(incomplete.missing, ", "), "field", incomplete.missing[0])
		return
	case err != nil:
		internalError(w, "save driver "+caller.Subject, err)
		return
	}
	profileUpdates.WithLabelValues("driver").Inc()
	if vehicleChanged {
		log.Printf("driver %s changed vehicle: back to PENDING, out of service until approved", caller.Subject)
	} else {
		log.Printf("driver %s updated their profile", caller.Subject)
	}
	writeJSON(w, http.StatusOK, newDriverView(d))
}

// handleApply serves POST /drivers/{id}/application: submit the saved profile for review.
// 201 when the driver is now PENDING; 200 with no change when they already were PENDING or
// APPROVED (a retry is safe); 409 suspended when an admin suspended them; 422 when the
// profile lacks a name, vehicle type or plate.
func (s *service) handleApply(w http.ResponseWriter, r *http.Request) {
	caller, ok := own(w, r)
	if !ok {
		return
	}
	d, submitted, err := s.apply(r.Context(), caller.Subject)
	var incomplete *incompleteError
	switch {
	case errors.Is(err, errNoProfile):
		refuse(w, http.StatusUnprocessableEntity, "incomplete_profile", "save your profile (name, vehicle type, plate) before applying", "field", "name")
		return
	case errors.As(err, &incomplete):
		refuse(w, http.StatusUnprocessableEntity, "incomplete_profile", "fill in before applying: "+strings.Join(incomplete.missing, ", "), "field", incomplete.missing[0])
		return
	case errors.Is(err, errSuspended):
		refuse(w, http.StatusConflict, "suspended", "your account is suspended; contact support", "status", statusSuspended)
		return
	case err != nil:
		internalError(w, "apply driver "+caller.Subject, err)
		return
	}
	if !submitted {
		applications.WithLabelValues("replayed").Inc()
		writeJSON(w, http.StatusOK, newDriverView(d))
		return
	}
	applications.WithLabelValues("submitted").Inc()
	log.Printf("driver %s applied: PENDING", caller.Subject)
	writeJSON(w, http.StatusCreated, newDriverView(d))
}

// handlePublic serves GET /drivers/{id}/public: the name, vehicle and plate of an APPROVED
// driver, for the rider whose trip they are on. Anything else (no such driver, not approved,
// suspended) is the same 404, so the endpoint does not say who has applied. Never the phone.
// Limited per caller, because ids are easy to guess.
func (s *service) handlePublic(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	if s.publicLimiter != nil {
		if allowed, wait := s.publicLimiter.Allow(string(caller.Role) + ":" + caller.Subject); !allowed {
			seconds := max(1, int(math.Ceil(wait.Seconds())))
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			requestsRefused.WithLabelValues("rate_limited").Inc()
			// Also in the body, which a browser app can read without CORS exposing the header.
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "too many lookups, slow down", "code": "rate_limited", "retryAfter": seconds})
			return
		}
	}
	d, err := loadDriver(r.Context(), s.db, r.PathValue("id"), false)
	if err != nil && !errors.Is(err, errNoDriver) {
		internalError(w, "load public driver", err)
		return
	}
	if err != nil || d.Status != statusApproved {
		publicLookups.WithLabelValues("not_found").Inc()
		refuse(w, http.StatusNotFound, "not_found", "no such driver")
		return
	}
	publicLookups.WithLabelValues("found").Inc()
	writeJSON(w, http.StatusOK, publicDriverView{
		DriverID: d.ID, Name: d.Name, VehicleType: string(d.Vehicle), Plate: d.Plate, Model: d.Model, Color: d.Color,
	})
}

// --------------------------------------------------------------- reviews ----

func cursorFor(d driverRow) string {
	return fmt.Sprintf("%d.%s", d.StatusAt.UnixMicro(), d.ID)
}

func parseCursor(v string) (time.Time, string, error) {
	micros, id, ok := strings.Cut(v, ".")
	bad := errors.New("cursor must come from a previous nextCursor")
	if !ok || id == "" || len(id) > maxIDLen {
		return time.Time{}, "", bad
	}
	n, err := strconv.ParseInt(micros, 10, 64)
	if err != nil || n < 0 {
		return time.Time{}, "", bad
	}
	return time.UnixMicro(n).UTC(), id, nil
}

// handleList serves GET /driver-applications?status=PENDING&limit=&cursor=: the drivers with
// one status, the ones waiting longest first (admin only; includes phone numbers, because a
// reviewer may need to call). status defaults to PENDING. cursor is the nextCursor of the
// previous page, so pages neither skip nor repeat a driver while others are being decided.
func (s *service) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := statusPending
	if v := q.Get("status"); v != "" {
		switch v {
		case statusPending, statusApproved, statusRejected, statusSuspended:
			status = v
		default:
			refuse(w, http.StatusBadRequest, "invalid_request", "status must be PENDING, APPROVED, REJECTED or SUSPENDED")
			return
		}
	}
	limit := defaultPageSize
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPageSize {
			refuse(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit must be from 1 to %d", maxPageSize))
			return
		}
		limit = n
	}
	var afterAt time.Time
	var afterID string
	hasCursor := false
	if v := q.Get("cursor"); v != "" {
		var err error
		if afterAt, afterID, err = parseCursor(v); err != nil {
			refuse(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		hasCursor = true
	}
	rows, err := s.listDrivers(r.Context(), status, limit+1, afterAt, afterID, hasCursor)
	if err != nil {
		internalError(w, "list driver applications", err)
		return
	}
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		c := cursorFor(rows[limit-1])
		next = &c
	}
	views := make([]adminDriverView, len(rows))
	for i, d := range rows {
		views[i] = newAdminDriverView(d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"applications": views, "nextCursor": next})
}

type reviewRequest struct {
	Reason string `json:"reason"`
}

// handleReview serves POST /drivers/{id}/approve|reject|suspend (admin). reject and suspend
// need a reason of 1 to 500 characters, which the driver sees; approve may have one. The
// driver's status must allow the action (reviewActions), or it is a 409 naming the status
// they have. The decision and its driver.application.reviewed commit together.
func (s *service) handleReview(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := auth.From(r.Context())
		if !ok {
			auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		var req reviewRequest
		if !readBody(w, r, &req, true) {
			return
		}
		reason, ferr := cleanReason(req.Reason, reviewActions[action].needsReason)
		if ferr != nil {
			invalid(w, ferr)
			return
		}
		id := r.PathValue("id")
		if len(id) > maxIDLen || !utf8.ValidString(id) {
			refuse(w, http.StatusNotFound, "not_found", "no such driver")
			return
		}
		d, err := s.review(r.Context(), id, action, caller.Subject, reason)
		var te *transitionError
		switch {
		case errors.Is(err, errNoDriver):
			refuse(w, http.StatusNotFound, "not_found", "no such driver")
			return
		case errors.As(err, &te):
			refuse(w, http.StatusConflict, "invalid_transition", te.Error(), "status", te.from)
			return
		case err != nil:
			internalError(w, action+" driver "+id, err)
			return
		}
		log.Printf("driver %s: %s by %s -> %s", id, action, caller.Subject, d.Status)
		writeJSON(w, http.StatusOK, newAdminDriverView(d))
	}
}

type resyncRequest struct {
	DriverID string `json:"driverId"`
}

// handleResync serves POST /driver-applications/resync (admin): re-publishes the current
// status of every APPROVED, REJECTED or SUSPENDED driver as driver.application.reviewed, or of
// the one in the optional body {"driverId": "..."}, so Matching can rebuild what a Redis that
// lost its data forgot (see resync). 200 with the counts per status; 404 for an unknown
// driverId; 409 resync_running while another resync runs. The run finishes even if the caller
// goes away. For a very large fleet the response can outlast the server's write timeout: the
// work is done all the same, and `profile-service resync` has no such limit.
func (s *service) handleResync(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	var req resyncRequest
	if !readBody(w, r, &req, true) {
		return
	}
	if req.DriverID != "" && !validDriverID(req.DriverID) {
		refuse(w, http.StatusNotFound, "not_found", "no such driver")
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), resyncTimeout)
	defer cancel()
	res, err := s.resync(ctx, req.DriverID)
	switch {
	case errors.Is(err, errResyncRunning):
		refuse(w, http.StatusConflict, "resync_running", "a resync is already running; try again when it has finished")
		return
	case errors.Is(err, errNoDriver):
		refuse(w, http.StatusNotFound, "not_found", "no such driver")
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
		res.Total, res.Counts[statusApproved], res.Counts[statusRejected], res.Counts[statusSuspended], res.Skipped)
	writeJSON(w, http.StatusOK, map[string]any{"queued": res.Total, "counts": res.Counts, "skipped": res.Skipped})
}
