package app

import (
	"context"
	"errors"
	"time"

	"grab-mini/promotion-service/internal/domain"
)

// ValidateQuery is what a rider asks, as the transport received it: the raw
// strings, because the order in which they are refused is behaviour.
type ValidateQuery struct {
	RiderID, Code, Price, VehicleType string
}

// Validation is the answer for a code that applies.
type Validation struct {
	Code                        string
	Discount, Price, FinalPrice int64
	Currency, Token             string
	ExpiresAt                   time.Time
}

// Validate says whether the code applies to a trip priced at q.Price for the
// vehicle, and if so the discount and a token bound to rider, code, price and
// vehicle. Refusals, in order: a malformed code (not_found, counted), the
// price and the vehicle (*domain.InvalidRequestError, not counted), then the
// code itself (*domain.Rejection, counted under its code). Other errors are
// *StepError.
func (s *Service) Validate(ctx context.Context, q ValidateQuery) (Validation, error) {
	code := domain.NormalizeCode(q.Code)
	if !domain.ValidCode(code) {
		// Not even shaped like a code: the same answer as a code that does not exist.
		s.d.Metrics.Validation(domain.CodeNotFound)
		return Validation{}, notFound()
	}
	price, err := domain.ParsePrice(q.Price)
	if err != nil {
		return Validation{}, &domain.InvalidRequestError{Message: err.Error()}
	}
	vehicle := q.VehicleType
	if vehicle == "" {
		vehicle = domain.VehicleCar // as for an order
	}
	if !domain.ValidVehicle(vehicle) {
		return Validation{}, &domain.InvalidRequestError{Message: "vehicleType must be CAR or BIKE"}
	}

	p, err := s.d.Reader.Find(ctx, code)
	if errors.Is(err, domain.ErrNotFound) {
		s.d.Metrics.Validation(domain.CodeNotFound)
		return Validation{}, notFound()
	}
	if err != nil {
		return Validation{}, step("load promotion", err)
	}
	u, err := s.d.Reader.Usage(ctx, code, q.RiderID)
	if err != nil {
		return Validation{}, step("count redemptions", err)
	}
	now := s.d.Now()
	if rej := p.Check(price, vehicle, now, u); rej != nil {
		s.d.Metrics.Validation(rej.Code)
		return Validation{}, rej
	}

	discount := p.Discount(price)
	token, exp := s.d.Tokens.Issue(p.Code, q.RiderID, price, vehicle, discount, now)
	s.d.Metrics.Validation("ok")
	return Validation{
		Code: p.Code, Discount: discount, Price: price, FinalPrice: price - discount,
		Currency: domain.CurrencyVND, Token: token, ExpiresAt: exp,
	}, nil
}

func notFound() *domain.Rejection {
	return &domain.Rejection{Code: domain.CodeNotFound, Message: "no such promotion code"}
}
