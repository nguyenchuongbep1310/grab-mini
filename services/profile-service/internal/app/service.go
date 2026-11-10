// Package app holds the profile use cases. It knows the domain and its own ports, and
// nothing about HTTP, SQL or Kafka.
package app

import "time"

// Deps are the ports a Service needs. Relay may be nil (a command that only queues events).
type Deps struct {
	Riders  RiderStore
	Drivers DriverReader
	Queue   ApplicationQueue
	Resync  ResyncSource
	Locker  ResyncLocker
	UoW     UnitOfWork
	Encoder Encoder
	Relay   Waker
	Metrics Metrics
	Now     func() time.Time
}

// Service runs the use cases.
type Service struct {
	riders  RiderStore
	drivers DriverReader
	queue   ApplicationQueue
	resync  ResyncSource
	locker  ResyncLocker
	uow     UnitOfWork
	enc     Encoder
	relay   Waker
	metrics Metrics
	clock   func() time.Time
}

// New builds a Service; a nil Now is time.Now.
func New(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	return &Service{
		riders: d.Riders, drivers: d.Drivers, queue: d.Queue, resync: d.Resync, locker: d.Locker,
		uow: d.UoW, enc: d.Encoder, relay: d.Relay, metrics: d.Metrics, clock: d.Now,
	}
}

func (s *Service) now() time.Time { return s.clock().UTC().Truncate(time.Microsecond) }

func (s *Service) wake() {
	if s.relay != nil {
		s.relay.Wake()
	}
}
