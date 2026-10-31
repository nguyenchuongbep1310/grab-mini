package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

var driverReviews = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "matching_driver_reviews_total",
	Help: "driver.application.reviewed events handled, by what they did: available (approved, now AVAILABLE), approved_busy (approved while on an order or trip), offline (taken out of service now), after_trip (taken out of service while on an order or trip: finishes it first), stale (older than, or the same as, the last result applied).",
}, []string{"result"})

// invalidReview is a review result that can never be applied (no driver, an unknown
// status, an approval without a vehicle type). Permanent: straight to the DLQ.
type invalidReview struct{ reason string }

func (e *invalidReview) Error() string   { return "invalid driver review: " + e.reason }
func (e *invalidReview) Permanent() bool { return true }

// applyDriverReview makes Profile Service's decision real in Redis, where Matching
// keeps who may be offered a trip.
//
//   - APPROVED: the driver becomes eligible exactly as `seed-drivers` makes a dev
//     driver: AVAILABLE with their vehicle. A driver who is on an order or a trip is
//     not touched (driverstatus.ApplyReview decides inside one script, so a claim
//     cannot slip in between).
//   - REJECTED / SUSPENDED: the driver is marked out of service, which every claim
//     respects, and set OFFLINE if free. One on an order or a trip finishes it; the
//     release at its end leaves them OFFLINE.
//
// It is idempotent and safe out of order: results carry a time that increases per
// driver, and one that is not newer than the last applied is ignored.
//
// Matching never needs a profile to exist: a driver nobody reviewed (seeded straight
// into Redis, as the load test and e2e do) is matched as before.
func applyDriverReview(ctx context.Context, env Env, rdb *redis.Client, evt events.DriverApplicationReviewed) error {
	if evt.DriverID == "" {
		return &invalidReview{"no driverId"}
	}
	var approved bool
	switch evt.Status {
	case events.ReviewApproved:
		approved = true
		if !evt.VehicleType.Valid() {
			return &invalidReview{fmt.Sprintf("driver %s approved with vehicle type %q", evt.DriverID, evt.VehicleType)}
		}
	case events.ReviewRejected, events.ReviewSuspended:
	default:
		return &invalidReview{fmt.Sprintf("driver %s: unknown status %q", evt.DriverID, evt.Status)}
	}
	at, err := time.Parse(time.RFC3339Nano, evt.OccurredAt)
	if err != nil {
		return &invalidReview{fmt.Sprintf("driver %s: occurredAt %q is not a time", evt.DriverID, evt.OccurredAt)}
	}

	result, err := driverstatus.ApplyReview(ctx, rdb, evt.DriverID, approved, string(evt.VehicleType), at)
	if err != nil {
		return err // transient: retried
	}
	driverReviews.WithLabelValues(result.String()).Inc()
	log.Printf("[%s] driver=%s review %s (event %s): %s", env.InstanceID, evt.DriverID, evt.Status, evt.EventID, result)
	return nil
}

// runReviewConsumer applies driver.application.reviewed. Its own consumer group, like
// the cancel consumer: a driver's approval must not wait behind a match that is retrying.
func runReviewConsumer(ctx context.Context, env Env, rdb *redis.Client, codec *eventcodec.Codec) {
	runReviewConsumerWith(ctx, reviewConsumerConfig(env), env, rdb, codec)
}

// runReviewConsumerWith is runReviewConsumer with a given consumer config (tests need a group of their own).
func runReviewConsumerWith(ctx context.Context, cfg kafkaconsumer.Config, env Env, rdb *redis.Client, codec *eventcodec.Codec) {
	kafkaconsumer.Run(ctx, cfg, func(ctx context.Context, msg kafka.Message) error {
		var evt events.DriverApplicationReviewed
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return applyDriverReview(ctx, env, rdb, evt)
	})
}

func reviewConsumerConfig(env Env) kafkaconsumer.Config {
	return kafkaconsumer.Config{
		Brokers: env.KafkaBrokers,
		// scripts/kafka-acls.sh grants this group and KEDA scales on it.
		GroupID:    "matching-service-driver-review-group",
		Topic:      events.TopicDriverApplicationReviewed,
		LogLabel:   "[" + env.InstanceID + "-review]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}
}
