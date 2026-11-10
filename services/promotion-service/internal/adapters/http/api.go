// Package http is promotion-service's HTTP API: routes, the JSON shapes (part
// of the contract with the apps) and the translation to and from the use
// cases. The use cases decide; this package reads the request, names the
// caller and writes the answer, byte for byte as the service always has.
package http

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
)

// API serves the promotion routes.
type API struct {
	svc      *app.Service
	verifier *auth.Verifier
}

// NewAPI returns the API over svc.
func NewAPI(svc *app.Service, verifier *auth.Verifier) *API {
	return &API{svc: svc, verifier: verifier}
}

// Routes is the HTTP API. Validation is for riders; creating, listing and
// disabling promotions is for admins only.
func (a *API) Routes() *http.ServeMux {
	// A rider can try codes as fast as they can type; a script that enumerates
	// them is guessing vouchers. Validation is cheap, so the limit is generous.
	checks := auth.LimiterFromEnv(2, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /promotions/validate", a.verifier.Require(checks.Wrap(http.HandlerFunc(a.handleValidate)), auth.Rider))
	mux.Handle("POST /promotions", a.verifier.Require(http.HandlerFunc(a.handleCreate), auth.Admin))
	mux.Handle("GET /promotions", a.verifier.Require(http.HandlerFunc(a.handleList), auth.Admin))
	mux.Handle("POST /promotions/{code}/disable", a.verifier.Require(http.HandlerFunc(a.handleDisable), auth.Admin))
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

// rejectionStatus is the HTTP status of each rejection: 404 for a code that
// does not exist, 422 for one that exists but cannot be used for this trip.
func rejectionStatus(code string) int {
	if code == domain.CodeNotFound {
		return http.StatusNotFound
	}
	return http.StatusUnprocessableEntity
}

// writeError answers a use case's error. A *app.StepError is a port failure
// and is tested first: a promotion that vanished after a write is a 500 of
// the step that read it, not a 404, even though it wraps domain.ErrNotFound.
func writeError(w http.ResponseWriter, what string, err error) {
	var step *app.StepError
	if errors.As(err, &step) {
		internalError(w, step.Step, step.Err)
		return
	}
	var rej *domain.Rejection
	var badReq *domain.InvalidRequestError
	var badPromo *domain.InvalidPromotionError
	switch {
	case errors.As(err, &rej):
		apiError(w, rejectionStatus(rej.Code), rej.Code, rej.Message)
	case errors.As(err, &badReq):
		apiError(w, http.StatusBadRequest, "invalid_request", badReq.Message)
	case errors.As(err, &badPromo):
		apiError(w, http.StatusBadRequest, "invalid_promotion", badPromo.Message)
	case errors.Is(err, domain.ErrCodeExists):
		apiError(w, http.StatusConflict, "code_exists", "a promotion with this code already exists")
	case errors.Is(err, domain.ErrNotFound):
		apiError(w, http.StatusNotFound, domain.CodeNotFound, "no such promotion code")
	default:
		internalError(w, what, err)
	}
}

// handleValidate serves GET /promotions/validate?code&price&vehicleType. It
// answers whether the code applies to a trip priced at price (whole VND) for
// the vehicle type, and if so the discount, the final price and a signed
// promoToken bound to this rider, code, price and vehicle. Order Service checks
// that token when the rider books.
func (a *API) handleValidate(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	q := r.URL.Query()
	v, err := a.svc.Validate(r.Context(), app.ValidateQuery{
		RiderID: caller.Subject, Code: q.Get("code"), Price: q.Get("price"), VehicleType: q.Get("vehicleType"),
	})
	if err != nil {
		writeError(w, "validate promotion", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"code":       v.Code,
		"discount":   v.Discount,
		"price":      v.Price,
		"finalPrice": v.FinalPrice,
		"currency":   v.Currency,
		"promoToken": v.Token,
		"expiresAt":  v.ExpiresAt,
	})
}

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

func toView(p app.PromotionView) promotionView {
	return promotionView{
		Code: p.Code, Kind: p.Kind, Value: p.Value, MaxDiscount: p.MaxDiscount, MinFare: p.MinFare,
		VehicleType: p.VehicleType, ValidFrom: p.ValidFrom, ValidTo: p.ValidTo, UsageLimit: p.UsageLimit,
		PerRider: p.PerRider, DisabledAt: p.DisabledAt, Status: p.Status, Redeemed: p.Redeemed,
		OverLimit: p.OverLimit, CreatedBy: p.CreatedBy, CreatedAt: p.CreatedAt,
	}
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

func (c createRequest) spec() domain.PromotionSpec {
	return domain.PromotionSpec{
		Code: c.Code, Kind: c.Kind, Value: c.Value, MaxDiscount: c.MaxDiscount, MinFare: c.MinFare,
		VehicleType: c.VehicleType, ValidFrom: c.ValidFrom, ValidTo: c.ValidTo, UsageLimit: c.UsageLimit,
		PerRider: c.PerRider,
	}
}

func (a *API) handleCreate(w http.ResponseWriter, r *http.Request) {
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
	v, err := a.svc.CreatePromotion(r.Context(), app.CreatePromotionCommand{AdminID: caller.Subject, Spec: req.spec()})
	if err != nil {
		writeError(w, "create promotion", err)
		return
	}
	writeJSON(w, http.StatusCreated, toView(v))
}

// handleList serves GET /promotions: every promotion, newest first, with how
// often it has been redeemed.
func (a *API) handleList(w http.ResponseWriter, r *http.Request) {
	list, err := a.svc.ListPromotions(r.Context())
	if err != nil {
		writeError(w, "list promotions", err)
		return
	}
	views := make([]promotionView, 0, len(list))
	for _, p := range list {
		views = append(views, toView(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"promotions": views})
}

// handleDisable serves POST /promotions/{code}/disable. Disabling twice is a
// success and keeps the first time. Redemptions already recorded, and orders
// that already carry the discount, are not touched: only new validations stop.
func (a *API) handleDisable(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	v, err := a.svc.DisablePromotion(r.Context(), caller.Subject, r.PathValue("code"))
	if err != nil {
		writeError(w, "disable promotion", err)
		return
	}
	writeJSON(w, http.StatusOK, toView(v))
}
