package main

import (
	"context"
	"grab-mini/pkg/redisconn"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/tracing"
)

var (
	matchOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "matching_orders_total",
		Help: "Orders matching finished with: matched, or no_driver.",
	}, []string{"result"})

	// How long a rider waits to hear a driver was found: from the moment the
	// order was created (its occurredAt) to the order.matched publish. The
	// SLO is 95% within 3 seconds.
	matchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "matching_latency_seconds",
		Help:    "Time from order.created to order.matched being published.",
		Buckets: []float64{.1, .25, .5, 1, 2, 3, 5, 10, 20, 30, 60},
	})
)

// publisher is the part of *kafka.Writer matching needs; tests substitute a fake.
type publisher interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// encoder is the part of *eventcodec.Codec matching needs.
type encoder interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

// matchOrder implements US-2.1: try to match, retrying for a bounded
// time before giving up and reporting no_driver_found.
func matchOrder(ctx context.Context, env Env, rdb *redis.Client, producer publisher, codec encoder, order events.OrderCreated) error {
	// An order matched by an earlier delivery (we may have died before
	// publishing, or before committing the offset) keeps its driver: claiming
	// again would take a second driver for the same order.
	driverID, err := driverstatus.MatchedDriver(ctx, rdb, order.OrderID)
	if err != nil {
		return err
	}
	if driverID != "" {
		log.Printf("[%s] order=%s was already matched to driver=%s, announcing it again", env.InstanceID, order.OrderID, driverID)
	}

	for attempt := 0; attempt <= env.MatchRetries && driverID == ""; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(env.MatchRetryDelay) * time.Millisecond):
			}
		}
		// The rider may have cancelled while we waited: don't take a driver for them.
		if skip, err := skipCancelled(ctx, env, rdb, order.OrderID); err != nil || skip {
			return err
		}
		driverID, err = findAndClaimNearestDriver(ctx, rdb, order.OrderID, order.Pickup.Lat, order.Pickup.Lng, env.MatchRadiusKm, order.VehicleType, env.AssignLease)
		if err != nil {
			return err
		}
		// A cancellation can land between the check and the claim. Hand the
		// driver straight back (and forget the match) instead of announcing them.
		if driverID != "" {
			if skip, err := skipCancelled(ctx, env, rdb, order.OrderID); err != nil || skip {
				if relErr := driverstatus.ReleaseMatch(ctx, rdb, order.OrderID, driverID); relErr != nil {
					log.Printf("[%s] failed to release driver=%s of cancelled order=%s: %v", env.InstanceID, driverID, order.OrderID, relErr)
				}
				return err
			}
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if driverID != "" {
		event := events.OrderMatched{
			Envelope:    events.Envelope{EventID: uuid.NewString(), OccurredAt: now},
			OrderID:     order.OrderID,
			DriverID:    driverID,
			VehicleType: order.VehicleType,
			RiderID:     order.RiderID,
		}
		publish := func() error {
			payload, err := codec.Encode(ctx, events.TopicOrderMatched, event)
			if err != nil {
				return err
			}
			started := time.Now()
			err = producer.WriteMessages(ctx, kafka.Message{
				Topic:   events.TopicOrderMatched,
				Key:     []byte(order.OrderID),
				Value:   payload,
				Headers: tracing.InjectKafka(ctx, nil),
			})
			metrics.ObservePublish(events.TopicOrderMatched, started, err)
			return err
		}
		if err := publish(); err != nil {
			// The driver is already claimed. Hand them back (and forget the
			// match), or a retry would claim a second driver and this one
			// stays ASSIGNED until the lease runs out.
			if relErr := driverstatus.ReleaseMatch(ctx, rdb, order.OrderID, driverID); relErr != nil {
				log.Printf("[%s] failed to release driver=%s after publish error: %v", env.InstanceID, driverID, relErr)
			}
			return err
		}
		log.Printf("[%s] matched order=%s -> driver=%s", env.InstanceID, order.OrderID, driverID)
		matchOutcomes.WithLabelValues("matched").Inc()
		if created, err := time.Parse(time.RFC3339, order.OccurredAt); err == nil {
			matchLatency.Observe(time.Since(created).Seconds())
		}
		return nil
	}

	event := events.OrderNoDriverFound{
		Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: now},
		OrderID:  order.OrderID,
		RiderID:  order.RiderID,
	}
	payload, err := codec.Encode(ctx, events.TopicOrderNoDriverFound, event)
	if err != nil {
		return err
	}
	started := time.Now()
	err = producer.WriteMessages(ctx, kafka.Message{
		Topic:   events.TopicOrderNoDriverFound,
		Key:     []byte(order.OrderID),
		Value:   payload,
		Headers: tracing.InjectKafka(ctx, nil),
	})
	metrics.ObservePublish(events.TopicOrderNoDriverFound, started, err)
	if err != nil {
		return err
	}
	matchOutcomes.WithLabelValues("no_driver").Inc()
	log.Printf("[%s] no driver found for order=%s", env.InstanceID, order.OrderID)
	return nil
}

func runConsumer(ctx context.Context, env Env) {
	rdb := redisconn.MustNew(env.RedisAddr)
	defer rdb.Close()

	producer := kafkaconn.Writer(&kafka.Writer{
		Addr:                   kafka.TCP(env.KafkaBrokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	})
	defer producer.Close()

	codec := eventcodec.New(env.SchemaRegistryURL)

	go health.Serve(ctx, env.HealthPort, health.Redis(rdb), health.Kafka(env.KafkaBrokers))

	// Returns when ctx is cancelled, like the order.created consumer below,
	// and is waited for so the Redis client isn't closed under it.
	var cancels sync.WaitGroup
	cancels.Go(func() { runCancelConsumer(ctx, env, rdb, codec) })
	defer cancels.Wait()

	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "matching-service-group",
		Topic:      events.TopicOrderCreated,
		LogLabel:   "[" + env.InstanceID + "]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
		// Redoing the last few orders after a crash is a no-op: the match is
		// recorded per order (driverstatus.ClaimForOrder). A synchronous commit
		// after every order is not a cost to ignore: a reader's commits go through
		// one loop, so they cap its throughput (about 200 orders a second here)
		// however many workers it has.
		CommitInterval: 100 * time.Millisecond,
	}, func(ctx context.Context, msg kafka.Message) error {
		var order events.OrderCreated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &order); err != nil {
			return err
		}
		return matchOrder(ctx, env, rdb, producer, codec, order)
	})
}

func main() {
	logging.Setup("matching-service")
	env := loadEnv()

	if len(os.Args) > 1 && os.Args[1] == "seed-drivers" {
		runSeedDrivers(env)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "matching-service")()
	runConsumer(ctx, env)
}
