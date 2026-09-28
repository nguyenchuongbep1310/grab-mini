package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
)

// matchOrder implements US-2.1: try to match, retrying for a bounded
// time before giving up and reporting no_driver_found.
func matchOrder(ctx context.Context, env Env, rdb *redis.Client, producer *kafka.Writer, order events.OrderCreated) error {
	var driverID string
	var err error

	for attempt := 0; attempt <= env.MatchRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(env.MatchRetryDelay) * time.Millisecond)
		}
		driverID, err = findAndClaimNearestDriver(ctx, rdb, order.Pickup.Lat, order.Pickup.Lng, env.MatchRadiusKm)
		if err != nil {
			return err
		}
		if driverID != "" {
			break
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if driverID != "" {
		event := events.OrderMatched{
			Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: now},
			OrderID:  order.OrderID,
			DriverID: driverID,
		}
		payload, _ := json.Marshal(event)
		if err := producer.WriteMessages(ctx, kafka.Message{
			Topic: events.TopicOrderMatched,
			Key:   []byte(order.OrderID),
			Value: payload,
		}); err != nil {
			return err
		}
		log.Printf("[%s] matched order=%s -> driver=%s", env.InstanceID, order.OrderID, driverID)
		return nil
	}

	event := events.OrderNoDriverFound{
		Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: now},
		OrderID:  order.OrderID,
	}
	payload, _ := json.Marshal(event)
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

func runConsumer(env Env) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: env.KafkaBrokers,
		GroupID: "matching-service-group",
		Topic:   events.TopicOrderCreated,
	})
	defer reader.Close()

	producer := &kafka.Writer{
		Addr:     kafka.TCP(env.KafkaBrokers...),
		Balancer: &kafka.Hash{},
	}
	defer producer.Close()

	log.Printf("[%s] matching-service consumer running", env.InstanceID)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}

		var order events.OrderCreated
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("failed to unmarshal order.created: %v", err)
			continue
		}

		log.Printf("[%s] received order.created partition=%d offset=%d orderId=%s",
			env.InstanceID, msg.Partition, msg.Offset, order.OrderID)

		if err := matchOrder(ctx, env, rdb, producer, order); err != nil {
			log.Printf("failed to match order %s: %v", order.OrderID, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit offset: %v", err)
		}
	}
}

func main() {
	env := loadEnv()

	if len(os.Args) > 1 && os.Args[1] == "seed-drivers" {
		runSeedDrivers(env)
		return
	}

	runConsumer(env)
}
