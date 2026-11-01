package app

import (
	"time"

	"grab-mini/payment-service/internal/domain"
)

// Service is the set of use cases. It holds the ports and the policy, and no
// state of its own.
type Service struct {
	orders      OrderStore
	assignments AssignmentStore
	payments    PaymentLookup
	uow         UnitOfWork
	enc         PaymentEncoder
	reader      Reader
	provider    Provider
	policy      domain.Policy
	metrics     Metrics
	now         func() time.Time
}

// Deps are what NewService needs. Now defaults to time.Now.
type Deps struct {
	Orders      OrderStore
	Assignments AssignmentStore
	Payments    PaymentLookup
	UnitOfWork  UnitOfWork
	Encoder     PaymentEncoder
	Reader      Reader
	Provider    Provider
	Policy      domain.Policy
	Metrics     Metrics
	Now         func() time.Time
}

func NewService(d Deps) *Service {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	return &Service{
		orders: d.Orders, assignments: d.Assignments, payments: d.Payments, uow: d.UnitOfWork,
		enc: d.Encoder, reader: d.Reader, provider: d.Provider, policy: d.Policy, metrics: d.Metrics, now: now,
	}
}
