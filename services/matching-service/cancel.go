package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// cancelledTTL is how long "the rider cancelled order X" is remembered: as
// long as a match is (driverstatus.MatchRecordTTL), which is how long a
// redelivered order.created can still turn up.
const cancelledTTL = driverstatus.MatchRecordTTL

func cancelledKey(orderID string) string {
	return fmt.Sprintf("order:%s:cancelled", orderID)
}

// markOrderCancelled records that the rider cancelled the order, so matching
// stops looking for a driver for it.
func markOrderCancelled(ctx context.Context, rdb *redis.Client, orderID string) error {
	return rdb.Set(ctx, cancelledKey(orderID), "1", cancelledTTL).Err()
}

// orderIsCancelled says whether the rider cancelled the order.
func orderIsCancelled(ctx context.Context, rdb *redis.Client, orderID string) (bool, error) {
	n, err := rdb.Exists(ctx, cancelledKey(orderID)).Result()
	return n == 1, err
}

// runCancelConsumer writes the cancelled-order key for every order.cancelled.
// It is its own consumer group: it runs alongside the order.created consumer
// and must not wait behind a match that is retrying.
func runCancelConsumer(ctx context.Context, env Env, rdb *redis.Client, codec *eventcodec.Codec) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "matching-service-cancel-group",
		Topic:      events.TopicOrderCancelled,
		LogLabel:   "[" + env.InstanceID + "-cancel]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.OrderCancelled
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		if err := markOrderCancelled(ctx, rdb, evt.OrderID); err != nil {
			return err
		}
		log.Printf("[%s] order=%s cancelled by the rider: matching will skip it", env.InstanceID, evt.OrderID)
		return nil
	})
}

// skipCancelled reports whether matching should give up on the order because
// the rider cancelled it. No event is published: Order Service already told
// everyone with order.cancelled.
func skipCancelled(ctx context.Context, env Env, rdb *redis.Client, orderID string) (bool, error) {
	cancelled, err := orderIsCancelled(ctx, rdb, orderID)
	if err != nil || !cancelled {
		return false, err
	}
	log.Printf("[%s] order=%s was cancelled, not matching it", env.InstanceID, orderID)
	matchOutcomes.WithLabelValues("cancelled").Inc()
	return true, nil
}
