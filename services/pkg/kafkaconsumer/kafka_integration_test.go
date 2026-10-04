package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
)

// These tests run against a real broker: set TEST_KAFKA_BROKERS (e.g.
// localhost:9092). Each test uses topics and groups with fresh names.
func brokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	return []string{b}
}

func newTopic(t *testing.T, brokers []string, partitions int) string {
	t.Helper()
	topic := "t-" + uuid.NewString()[:8]
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1},
			// One partition, so a test can read everything dead-lettered.
			{Topic: events.DeadLetterTopic(topic), NumPartitions: 1, ReplicationFactor: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range resp.Errors {
		if e != nil {
			t.Fatalf("create %s: %v", name, e)
		}
	}
	return topic
}

func publish(t *testing.T, brokers []string, topic string, key, value string) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(value)}); err != nil {
		t.Fatal(err)
	}
}

// start runs a consumer for topic until the test ends and returns a stop
// function that waits for Run to return.
func start(t *testing.T, cfg Config, h Handler) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, cfg, h); close(done) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("Run did not return after its context was cancelled")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func config(brokers []string, topic string) Config {
	return Config{
		Brokers:    brokers,
		GroupID:    "g-" + uuid.NewString()[:8],
		Topic:      topic,
		RetryDelay: 10 * time.Millisecond,
		RetryTiers: []time.Duration{150 * time.Millisecond, 150 * time.Millisecond},
		Partitions: 1,
		LogLabel:   "[test]",
	}
}

type call struct {
	topic, value string
	at           time.Time
}

type recorder struct {
	mu    sync.Mutex
	calls []call
	ch    chan call
}

func newRecorder() *recorder { return &recorder{ch: make(chan call, 100)} }

func (r *recorder) record(msg kafka.Message) {
	c := call{msg.Topic, string(msg.Value), time.Now()}
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
	r.ch <- c
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func readDLQ(t *testing.T, brokers []string, topic string) dlqEnvelope {
	t.Helper()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: events.DeadLetterTopic(topic), Partition: 0, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	m, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("nothing arrived on the dead-letter topic: %v", err)
	}
	var env dlqEnvelope
	if err := json.Unmarshal(m.Value, &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestATransientFailureIsRetriedThroughTheTiersUntilItSucceeds(t *testing.T) {
	b := brokers(t)
	topic := newTopic(t, b, 1)
	rec := newRecorder()
	var n int
	var mu sync.Mutex
	start(t, config(b, topic), func(_ context.Context, msg kafka.Message) error {
		rec.record(msg)
		mu.Lock()
		defer mu.Unlock()
		n++
		if n <= 3 { // two tries in place, then the first retry topic fail too
			return errors.New("database is down")
		}
		return nil
	})

	publish(t, b, topic, "k", "hello")
	waitFor(t, "four attempts", func() bool { return rec.count() == 4 })

	for i, c := range rec.calls {
		if c.topic != topic || c.value != "hello" {
			t.Errorf("attempt %d saw topic=%q value=%q; a retry must look like the original message", i+1, c.topic, c.value)
		}
	}
	// The third and fourth attempts came out of retry topics, so they were
	// delayed by the tier (150ms each), not immediate.
	if gap := rec.calls[3].at.Sub(rec.calls[2].at); gap < 100*time.Millisecond {
		t.Errorf("tier 2 attempt came %s after tier 1, want it delayed by about 150ms", gap)
	}
	time.Sleep(500 * time.Millisecond)
	if rec.count() != 4 {
		t.Errorf("%d attempts, want it to stop at 4 once it succeeded", rec.count())
	}
}

func TestAMessageThatKeepsFailingEndsUpOnTheDeadLetterTopic(t *testing.T) {
	b := brokers(t)
	topic := newTopic(t, b, 1)
	rec := newRecorder()
	start(t, config(b, topic), func(_ context.Context, msg kafka.Message) error {
		rec.record(msg)
		return errors.New("still down")
	})

	publish(t, b, topic, "k", "doomed")
	env := readDLQ(t, b, topic)

	if env.OriginalTopic != topic || env.ValueText != "doomed" || env.Retries != 2 {
		t.Errorf("DLQ envelope = %+v, want the original topic and value after 2 retries", env)
	}
	// 2 in place + 1 per tier.
	if got := rec.count(); got != 4 {
		t.Errorf("handler ran %d times, want 4", got)
	}
}

func TestAPermanentErrorGoesStraightToTheDeadLetterTopic(t *testing.T) {
	b := brokers(t)
	topic := newTopic(t, b, 1)
	rec := newRecorder()
	start(t, config(b, topic), func(_ context.Context, msg kafka.Message) error {
		rec.record(msg)
		return permErr{}
	})

	publish(t, b, topic, "k", "garbage")
	env := readDLQ(t, b, topic)

	if env.Retries != 0 || rec.count() != 1 {
		t.Errorf("retries=%d calls=%d, want a permanent error tried once and dead-lettered", env.Retries, rec.count())
	}
}

// The point of retry topics: a message waiting for its retry must not hold up
// the ones behind it.
func TestAFailingMessageDoesNotHoldUpTheOnesBehindIt(t *testing.T) {
	b := brokers(t)
	topic := newTopic(t, b, 1) // one partition: B is strictly behind A
	cfg := config(b, topic)
	cfg.RetryTiers = []time.Duration{3 * time.Second}
	rec := newRecorder()
	start(t, cfg, func(_ context.Context, msg kafka.Message) error {
		rec.record(msg)
		if string(msg.Value) == "A" {
			return errors.New("not yet")
		}
		return nil
	})

	started := time.Now()
	publish(t, b, topic, "k", "A")
	publish(t, b, topic, "k", "B")

	waitFor(t, "B to be handled", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, c := range rec.calls {
			if c.value == "B" {
				return true
			}
		}
		return false
	})
	if waited := time.Since(started); waited > 2500*time.Millisecond {
		t.Errorf("B waited %s: it was held up by A's retry", waited)
	}
}

func TestShuttingDownStopsEveryReader(t *testing.T) {
	b := brokers(t)
	topic := newTopic(t, b, 1)
	stop := start(t, config(b, topic), func(context.Context, kafka.Message) error { return nil })
	time.Sleep(500 * time.Millisecond)

	begun := time.Now()
	stop() // fails the test itself if Run doesn't return
	if took := time.Since(begun); took > 8*time.Second {
		t.Errorf("shutdown took %s", took)
	}
}

func TestAMessageInterruptedByShutdownIsDeliveredAgain(t *testing.T) {
	b := brokers(t)
	topic := newTopic(t, b, 1)
	cfg := config(b, topic)
	cfg.RetryTiers = nil       // retry in place...
	cfg.RetryDelay = time.Hour // ...and wait so long between tries that we are surely asleep when we stop

	inHandler := make(chan struct{})
	stop := start(t, cfg, func(ctx context.Context, msg kafka.Message) error {
		select {
		case inHandler <- struct{}{}:
		default:
		}
		return errors.New("fails, so Run sleeps before retrying")
	})

	publish(t, b, topic, "k", "survivor")
	<-inHandler
	stop()

	// A new consumer in the same group picks the message up again, because the
	// first one never committed it.
	rec := newRecorder()
	start(t, Config{Brokers: b, GroupID: cfg.GroupID, Topic: topic, LogLabel: "[second]"}, func(_ context.Context, msg kafka.Message) error {
		rec.record(msg)
		return nil
	})
	waitFor(t, "redelivery", func() bool { return rec.count() >= 1 })
}
