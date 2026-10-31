package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/promo"
)

// routes is the HTTP API. Validation is for riders; creating, listing and
// disabling promotions is for admins only.
func (s *service) routes(verifier *auth.Verifier) *http.ServeMux {
	// A rider can try codes as fast as they can type; a script that enumerates
	// them is guessing vouchers. Validation is cheap, so the limit is generous.
	checks := auth.LimiterFromEnv(2, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /promotions/validate", verifier.Require(checks.Wrap(http.HandlerFunc(s.handleValidate)), auth.Rider))
	mux.Handle("POST /promotions", verifier.Require(http.HandlerFunc(s.handleCreate), auth.Admin))
	mux.Handle("GET /promotions", verifier.Require(http.HandlerFunc(s.handleList), auth.Admin))
	mux.Handle("POST /promotions/{code}/disable", verifier.Require(http.HandlerFunc(s.handleDisable), auth.Admin))
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// apiError is the body of every refusal: a message for people and a stable
// code for programs.
func apiError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}

func internalError(w http.ResponseWriter, what string, err error) {
	log.Printf("%s: %v", what, err)
	apiError(w, http.StatusInternalServerError, "internal", "internal error")
}

var codePattern = regexp.MustCompile(`^[A-Z0-9_-]{3,32}$`)

// normalizeCode upper-cases and trims what a rider typed.
func normalizeCode(v string) string { return strings.ToUpper(strings.TrimSpace(v)) }

// ---------------------------------------------------------------- validate ----

// rejectionStatus is the HTTP status of each rejection: 404 for a code that
// does not exist, 422 for one that exists but cannot be used for this trip.
func rejectionStatus(code string) int {
	if code == codeNotFound {
		return http.StatusNotFound
	}
	return http.StatusUnprocessableEntity
}

// handleValidate serves GET /promotions/validate?code&price&vehicleType. It
// answers whether the code applies to a trip priced at price (whole VND) for
// the vehicle type, and if so the discount, the final price and a signed
// promoToken bound to this rider, code, price and vehicle. Order Service checks
// that token when the rider books.
func (s *service) handleValidate(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	q := r.URL.Query()
	code := normalizeCode(q.Get("code"))
	if !codePattern.MatchString(code) {
		// Not even shaped like a code: the same answer as a code that does not exist.
		validations.WithLabelValues(codeNotFound).Inc()
		apiError(w, http.StatusNotFound, codeNotFound, "no such promotion code")
		return
	}
	price, err := parsePrice(q.Get("price"))
	if err != nil {
		apiError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	vehicle := q.Get("vehicleType")
	if vehicle == "" {
		vehicle = string(events.VehicleCar) // as for an order
	}
	if !events.VehicleType(vehicle).Valid() {
		apiError(w, http.StatusBadRequest, "invalid_request", "vehicleType must be CAR or BIKE")
		return
	}

	ctx := r.Context()
	p, err := findPromotion(ctx, s.db, code, false)
	if errors.Is(err, errNoPromotion) {
		validations.WithLabelValues(codeNotFound).Inc()
		apiError(w, http.StatusNotFound, codeNotFound, "no such promotion code")
		return
	}
	if err != nil {
		internalError(w, "load promotion", err)
		return
	}
	u, err := usage(ctx, s.db, code, caller.Subject)
	if err != nil {
		internalError(w, "count redemptions", err)
		return
	}
	now := s.now()
	if rej := p.Check(price, vehicle, now, u); rej != nil {
		validations.WithLabelValues(rej.Code).Inc()
		apiError(w, rejectionStatus(rej.Code), rej.Code, rej.Message)
		return
	}

	discount := p.Discount(price)
	token, exp := s.signer.Issue(p.Code, promo.Binding{RiderID: caller.Subject, Price: price, Vehicle: vehicle}, discount, now)
	validations.WithLabelValues("ok").Inc()
	writeJSON(w, http.StatusOK, map[string]any{
		"code":       p.Code,
		"discount":   discount,
		"price":      price,
		"finalPrice": price - discount,
		"currency":   currencyVND,
		"promoToken": token,
		"expiresAt":  exp,
	})
}

// parsePrice reads the trip price in VND. A client may send 87000 or 87000.0;
// it is rounded to a whole dong, and must be positive and not absurd.
func parsePrice(v string) (int64, error) {
	if v == "" {
		return 0, errors.New("price is required")
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, errors.New("price must be a number of VND")
	}
	if f <= 0 || f > float64(maxAmount) {
		return 0, errors.New("price must be above 0 VND")
	}
	return int64(math.Round(f)), nil
}

// ------------------------------------------------------------- admin: CRUD ----

// promotionView is a promotion as an admin sees it.
type promotionView struct {
	Code        string     `json:"code"`
	Kind        string     `json:"kind"`
	Value       int64      `json:"value"`
	MaxDiscount *int64     `json:"maxDiscount"`
	MinFare     int64      `json:"minFare"`
	VehicleType *string    `json:"vehicleType"`
	ValidFrom   time.Time  `json:"validFrom"`
	ValidTo     time.Time  `json:"validTo"`
	UsageLimit  *int64     `json:"usageLimit"`
	PerRider    int64      `json:"perRiderLimit"`
	DisabledAt  *time.Time `json:"disabledAt"`
	// scheduled, active, expired or disabled, as of now.
	Status string `json:"status"`
	// Redemptions that count (REDEEMED), and orders recorded past a limit (OVER_LIMIT).
	Redeemed  int64     `json:"redeemed"`
	OverLimit int64     `json:"overLimit"`
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
}

func promotionStatus(p Promotion, now time.Time) string {
	switch {
	case p.DisabledAt != nil:
		return "disabled"
	case now.Before(p.ValidFrom):
		return "scheduled"
	case !now.Before(p.ValidTo):
		return "expired"
	}
	return "active"
}

const viewColumns = `p.code, p.kind, p.value, p.max_discount, p.min_fare, p.vehicle_type, p.valid_from, p.valid_to,
	p.usage_limit, p.per_rider_limit, p.disabled_at, p.created_by, p.created_at,
	count(r.*) FILTER (WHERE r.status = 'REDEEMED'), count(r.*) FILTER (WHERE r.status = 'OVER_LIMIT')`

func (s *service) scanView(row scanner) (promotionView, error) {
	var v promotionView
	err := row.Scan(&v.Code, &v.Kind, &v.Value, &v.MaxDiscount, &v.MinFare, &v.VehicleType, &v.ValidFrom, &v.ValidTo,
		&v.UsageLimit, &v.PerRider, &v.DisabledAt, &v.CreatedBy, &v.CreatedAt, &v.Redeemed, &v.OverLimit)
	if err != nil {
		return v, err
	}
	v.Status = promotionStatus(Promotion{ValidFrom: v.ValidFrom, ValidTo: v.ValidTo, DisabledAt: v.DisabledAt}, s.now())
	return v, nil
}

type createRequest struct {
	Code        string     `json:"code"`
	Kind        string     `json:"kind"`
	Value       int64      `json:"value"`
	MaxDiscount *int64     `json:"maxDiscount"`
	MinFare     int64      `json:"minFare"`
	VehicleType *string    `json:"vehicleType"`
	ValidFrom   *time.Time `json:"validFrom"`
	ValidTo     time.Time  `json:"validTo"`
	UsageLimit  *int64     `json:"usageLimit"`
	PerRider    *int64     `json:"perRiderLimit"`
}

// validate returns the promotion the request describes, or why it cannot be one.
func (c createRequest) promotion(now time.Time) (Promotion, string) {
	p := Promotion{
		Code: normalizeCode(c.Code), Kind: strings.ToUpper(strings.TrimSpace(c.Kind)), Value: c.Value,
		MaxDiscount: c.MaxDiscount, MinFare: c.MinFare, VehicleType: c.VehicleType,
		ValidFrom: now, ValidTo: c.ValidTo, UsageLimit: c.UsageLimit, PerRider: 1,
	}
	if c.ValidFrom != nil {
		p.ValidFrom = *c.ValidFrom
	}
	if c.PerRider != nil {
		p.PerRider = *c.PerRider
	}
	switch {
	case !codePattern.MatchString(p.Code):
		return p, "code must be 3 to 32 characters: letters, digits, '-' or '_'"
	case p.Kind != kindPercent && p.Kind != kindFixed:
		return p, "kind must be PERCENT or FIXED"
	case p.Value <= 0 || p.Value > maxAmount:
		return p, "value must be above 0"
	case p.Kind == kindPercent && p.Value > 100:
		return p, "a PERCENT value is a percentage from 1 to 100"
	case p.MaxDiscount != nil && p.Kind != kindPercent:
		return p, "maxDiscount applies to PERCENT promotions only"
	case p.MaxDiscount != nil && (*p.MaxDiscount <= 0 || *p.MaxDiscount > maxAmount):
		return p, "maxDiscount must be above 0"
	case p.MinFare < 0 || p.MinFare > maxAmount:
		return p, "minFare must be 0 or more"
	case p.VehicleType != nil && !events.VehicleType(*p.VehicleType).Valid():
		return p, "vehicleType must be CAR or BIKE (or omitted for any)"
	case p.ValidTo.IsZero():
		return p, "validTo is required"
	case !p.ValidTo.After(p.ValidFrom):
		return p, "validTo must be after validFrom"
	case !p.ValidTo.After(now):
		return p, "validTo is in the past"
	case p.UsageLimit != nil && (*p.UsageLimit <= 0 || *p.UsageLimit > maxAmount):
		return p, "usageLimit must be above 0 (or omitted for no limit)"
	case p.PerRider <= 0 || p.PerRider > maxAmount:
		return p, "perRiderLimit must be 1 or more"
	}
	return p, ""
}

func (s *service) handleCreate(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	var req createRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	p, why := req.promotion(s.now())
	if why != "" {
		apiError(w, http.StatusBadRequest, "invalid_promotion", why)
		return
	}
	_, err := s.db.Exec(r.Context(), `
		INSERT INTO promotions (code, kind, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, usage_limit, per_rider_limit, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		p.Code, p.Kind, p.Value, p.MaxDiscount, p.MinFare, p.VehicleType, p.ValidFrom, p.ValidTo, p.UsageLimit, p.PerRider, caller.Subject)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		apiError(w, http.StatusConflict, "code_exists", "a promotion with this code already exists")
		return
	}
	if err != nil {
		internalError(w, "create promotion", err)
		return
	}
	v, err := s.loadView(r.Context(), p.Code)
	if err != nil {
		internalError(w, "load promotion", err)
		return
	}
	log.Printf("promotion %s created by %s", p.Code, caller.Subject)
	writeJSON(w, http.StatusCreated, v)
}

func (s *service) loadView(ctx context.Context, code string) (promotionView, error) {
	return s.scanView(s.db.QueryRow(ctx, `
		SELECT `+viewColumns+` FROM promotions p LEFT JOIN redemptions r ON r.code = p.code
		WHERE p.code = $1 GROUP BY p.code`, code))
}

// handleList serves GET /promotions: every promotion, newest first, with how
// often it has been redeemed.
func (s *service) handleList(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
		SELECT `+viewColumns+` FROM promotions p LEFT JOIN redemptions r ON r.code = p.code
		GROUP BY p.code ORDER BY p.created_at DESC, p.code LIMIT 500`)
	if err != nil {
		internalError(w, "list promotions", err)
		return
	}
	defer rows.Close()
	views := []promotionView{}
	for rows.Next() {
		v, err := s.scanView(rows)
		if err != nil {
			internalError(w, "read promotion", err)
			return
		}
		views = append(views, v)
	}
	if err := rows.Err(); err != nil {
		internalError(w, "list promotions", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"promotions": views})
}

// handleDisable serves POST /promotions/{code}/disable. Disabling twice is a
// success and keeps the first time. Redemptions already recorded, and orders
// that already carry the discount, are not touched: only new validations stop.
func (s *service) handleDisable(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	code := normalizeCode(r.PathValue("code"))
	tag, err := s.db.Exec(r.Context(), `UPDATE promotions SET disabled_at = COALESCE(disabled_at, $2) WHERE code = $1`, code, s.now())
	if err != nil {
		internalError(w, "disable promotion", err)
		return
	}
	if tag.RowsAffected() == 0 {
		apiError(w, http.StatusNotFound, codeNotFound, "no such promotion code")
		return
	}
	v, err := s.loadView(r.Context(), code)
	if err != nil {
		internalError(w, "load promotion", err)
		return
	}
	log.Printf("promotion %s disabled by %s", code, caller.Subject)
	writeJSON(w, http.StatusOK, v)
}
