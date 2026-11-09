package app

// Service is the set of order use cases. It holds the ports and no state of
// its own.
type Service struct {
	orders   OrderReader
	keys     KeyStore
	outcomes Outcomes
	uow      UnitOfWork
	enc      Encoder
	receipt  ReceiptEncoder
	pricer   *Pricer
	promos   *PromoChecker
	relay    Waker
	metrics  Metrics
}

// Deps are what New needs. Every field is required.
type Deps struct {
	Orders   OrderReader
	Keys     KeyStore
	Outcomes Outcomes
	UoW      UnitOfWork
	Encoder  Encoder
	Receipt  ReceiptEncoder
	Pricer   *Pricer
	Promos   *PromoChecker
	Relay    Waker
	Metrics  Metrics
}

func New(d Deps) *Service {
	return &Service{
		orders: d.Orders, keys: d.Keys, outcomes: d.Outcomes, uow: d.UoW, enc: d.Encoder, receipt: d.Receipt,
		pricer: d.Pricer, promos: d.Promos, relay: d.Relay, metrics: d.Metrics,
	}
}
