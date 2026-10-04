// Command location-gateway is how a driver's phone reports where it is.
//
// The Driver App must never talk to Kafka: a broker credential on a phone is a
// credential anyone can extract, and a phone can't be trusted about its own
// clock, rate or coordinates. The gateway sits between them. It authenticates
// the driver (the id comes from the token, not the request), validates the
// position, limits how often each driver may report, drops reports that say
// nothing new, stamps a sane timestamp, and publishes driver.location.updated
// in batches.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/serve"
)

// publisher is the part of *kafka.Writer the gateway needs.
type publisher interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// encoder is the part of *eventcodec.Codec the gateway needs.
type encoder interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

type gateway struct {
	tracker *tracker
	pub     publisher
	enc     encoder
	maxSkew time.Duration
	now     func() time.Time
}

type report struct {
	// Pointers so a missing coordinate is told apart from 0.
	Lat *float64 `json:"lat"`
	Lng *float64 `json:"lng"`
	// The device's own clock, optional (RFC 3339). Used only if it is close to ours.
	Timestamp string `json:"timestamp"`
}

func valid(lat, lng float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lng) && math.Abs(lat) <= 90 && math.Abs(lng) <= 180
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// stamp is the timestamp to publish: the device's, if it is within maxSkew of
// ours, and ours otherwise. Driver Location Service uses it to throw away
// updates that arrive out of order, so a phone with a wrong clock (or one
// lying) must not be able to jump a position into the future and make every
// later, honest update look old.
func (g *gateway) stamp(device string, now time.Time) time.Time {
	if device != "" {
		if ts, err := time.Parse(time.RFC3339, device); err == nil {
			if d := ts.Sub(now); d > -g.maxSkew && d < g.maxSkew {
				return ts.UTC()
			}
		}
	}
	return now.UTC()
}

func (g *gateway) handleReport(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	var rep report
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		auth.Deny(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if rep.Lat == nil || rep.Lng == nil || !valid(*rep.Lat, *rep.Lng) {
		auth.Deny(w, http.StatusBadRequest, "lat (-90 to 90) and lng (-180 to 180) are required")
		return
	}
	lat, lng := *rep.Lat, *rep.Lng

	now := g.now()
	d, wait := g.tracker.decide(caller.Subject, lat, lng, now)
	switch d {
	case tooFast:
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(wait.Seconds())))))
		auth.Deny(w, http.StatusTooManyRequests, "reporting too often")
		return
	case stationary:
		writeJSON(w, http.StatusAccepted, map[string]bool{"published": false})
		return
	}

	event := events.DriverLocationUpdated{
		Envelope:  events.Envelope{EventID: uuid.NewString(), OccurredAt: now.UTC().Format(time.RFC3339)},
		DriverID:  caller.Subject,
		Lat:       lat,
		Lng:       lng,
		Timestamp: g.stamp(rep.Timestamp, now).Format(time.RFC3339),
	}
	payload, err := g.enc.Encode(r.Context(), events.TopicDriverLocationUpdate, event)
	if err == nil {
		err = g.pub.WriteMessages(r.Context(), kafka.Message{Key: []byte(caller.Subject), Value: payload})
	}
	if err != nil {
		// Not published, so it must not count against the driver's rate limit
		// nor become the position later reports are compared with.
		g.tracker.refund(caller.Subject)
		log.Printf("driver=%s failed to publish location: %v", caller.Subject, err)
		auth.Deny(w, http.StatusServiceUnavailable, "could not accept the position, try again")
		return
	}
	g.tracker.sent(caller.Subject, lat, lng, now)
	writeJSON(w, http.StatusAccepted, map[string]bool{"published": true})
}

func (g *gateway) routes(verifier *auth.Verifier) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /locations", verifier.Require(http.HandlerFunc(g.handleReport), auth.Driver))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func main() {
	env := loadEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Telemetry: a lost position is replaced by the next one in seconds, so
	// one broker acknowledgement is enough. Messages are held for up to 20ms
	// to go out in batches: at thousands of reports a second, one request
	// per report would cost far more than the reports themselves.
	producer := kafkaconn.Writer(&kafka.Writer{
		Addr:                   kafka.TCP(env.KafkaBrokers...),
		Topic:                  events.TopicDriverLocationUpdate,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		BatchTimeout:           20 * time.Millisecond,
		BatchSize:              500,
		Compression:            kafka.Lz4,
		AllowAutoTopicCreation: true,
	})
	defer producer.Close()

	g := &gateway{
		tracker: newTracker(env.MinInterval, env.MinMove, env.Heartbeat),
		pub:     producer,
		enc:     eventcodec.New(env.SchemaRegistryURL),
		maxSkew: env.MaxSkew,
		now:     time.Now,
	}

	var background sync.WaitGroup
	background.Go(func() {
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				g.tracker.sweep(now, 10*time.Minute)
			}
		}
	})

	mux := g.routes(verifier)
	health.Register(ctx, mux, health.Kafka(env.KafkaBrokers))

	log.Printf("location-gateway listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, mux); err != nil {
		log.Fatal(err)
	}
	background.Wait()
	log.Printf("location-gateway stopped")
}
