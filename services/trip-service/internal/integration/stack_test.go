package integration

import (
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/outbox"
	httpadapter "grab-mini/trip-service/internal/adapters/http"
	kafkaadapter "grab-mini/trip-service/internal/adapters/kafka"
	tripmetrics "grab-mini/trip-service/internal/adapters/metrics"
	"grab-mini/trip-service/internal/adapters/postgres"
	redisadapter "grab-mini/trip-service/internal/adapters/redis"
	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
)

// migrationFiles is the service's own migrations directory (this package sits two levels below it).
var migrationFiles = os.DirFS("../..")

// stack is the service built the way main builds it, with the real Postgres
// and Redis adapters. The Kafka side is the Handler the consumers call, and the
// outbox relay writes to a recording writer instead of a broker.
type stack struct {
	t     *testing.T
	db    *pgxpool.Pool
	rdb   *redis.Client
	store *postgres.Store
	relay *outbox.Relay
	w     *recordingWriter
	codec *eventcodec.Codec

	svc    *app.Service
	events *kafkaadapter.Handler // events in: what the Kafka consumers call
	api    *httpadapter.API      // HTTP in
}

// newStack wires the service on db and rdb with the default proximity check on
// and the real clock (the configuration of a production stack).
func newStack(t *testing.T, db *pgxpool.Pool, rdb *redis.Client) *stack {
	t.Helper()
	w := &recordingWriter{}
	s := &stack{
		t: t, db: db, rdb: rdb, store: postgres.NewStore(db), w: w,
		relay: outbox.NewRelay(db, w, postgres.OutboxSource), codec: registry(t),
	}
	s.rewire(domain.DefaultProximity(), nil)
	return s
}

// rewire rebuilds the use cases, the event handler and the API with another
// proximity configuration and clock (nil is time.Now). The database, Redis and
// the relay are kept.
func (s *stack) rewire(prox domain.Proximity, now func() time.Time) {
	s.t.Helper()
	s.wire(prox, now, s.store)
}

// wire is rewire with a chosen trip reader (a test can put a stale one in front of the store).
func (s *stack) wire(prox domain.Proximity, now func() time.Time, trips app.TripReader) {
	s.svc = app.NewService(app.Deps{
		Trips: trips, Routes: s.store, Orders: s.store, Receipts: s.store,
		UnitOfWork: postgres.NewUnitOfWork(s.db, s.relay),
		Drivers:    redisadapter.NewDrivers(s.rdb),
		Locations:  redisadapter.NewLocations(s.rdb),
		Encoder:    kafkaadapter.NewEncoder(s.codec),
		Metrics:    tripmetrics.Prometheus{},
		Proximity:  prox, Now: now,
	})
	s.events = kafkaadapter.NewHandler(s.svc)
	s.api = httpadapter.New(s.svc)
}

func proximityOff() domain.Proximity {
	p := domain.DefaultProximity()
	p.Off = true
	return p
}

var (
	rider1  = domain.Actor{ID: "rider-1", Role: domain.RoleRider}
	driver1 = domain.Actor{ID: "driver-1", Role: domain.RoleDriver}
)

func driverOf(id string) domain.Actor { return domain.Actor{ID: id, Role: domain.RoleDriver} }
