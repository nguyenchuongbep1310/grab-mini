package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
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
		driverID, err = findAndClaimNearestDriver(ctx, rdb, order.OrderID, order.Pickup.Lat, order.Pickup.Lng, env.MatchRadiusKm, order.VehicleType, env.AssignLease)
		if err != nil {
			return err
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if driverID != "" {
		event := events.OrderMatched{
			Envelope:    events.Envelope{EventID: uuid.NewString(), OccurredAt: now},
			OrderID:     order.OrderID,
			DriverID:    driverID,
			VehicleType: order.VehicleType,
		}
		publish := func() error {
			payload, err := codec.Encode(ctx, events.TopicOrderMatched, event)
			if err != nil {
				return err
			}
			return producer.WriteMessages(ctx, kafka.Message{
				Topic: events.TopicOrderMatched,
				Key:   []byte(order.OrderID),
				Value: payload,
			})
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
		return nil
	}

	event := events.OrderNoDriverFound{
		Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: now},
		OrderID:  order.OrderID,
	}
	payload, err := codec.Encode(ctx, events.TopicOrderNoDriverFound, event)
	if err != nil {
		return err
	}
	if err := producer.WriteMessages(ctx, kafka.Message{
		Topic: events.TopicOrderNoDriverFound,
		Key:   []byte(order.OrderID),
		Value: payload,
	}); err != nil {
		return err
	}
	log.Printf("[%s] no driver found for order=%s", env.InstanceID, order.OrderID)
	return nil
}

func runConsumer(ctx context.Context, env Env) {
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(env.KafkaBrokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer producer.Close()

	codec := eventcodec.New(env.SchemaRegistryURL)

	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:  env.KafkaBrokers,
		GroupID:  "matching-service-group",
		Topic:    events.TopicOrderCreated,
		LogLabel: "[" + env.InstanceID + "]",
	}, func(ctx context.Context, msg kafka.Message) error {
		var order events.OrderCreated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &order); err != nil {
			return err
		}
		return matchOrder(ctx, env, rdb, producer, codec, order)
	})
}

func main() {
	env := loadEnv()

	if len(os.Args) > 1 && os.Args[1] == "seed-drivers" {
		runSeedDrivers(env)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runConsumer(ctx, env)
}
