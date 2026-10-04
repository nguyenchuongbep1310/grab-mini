package kafkaconsumer

import (
	"context"
	"time"

	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"testing"
)

type permErr struct{}

func (permErr) Error() string   { return "bad" }
func (permErr) Permanent() bool { return true }

type transientErr struct{}

func (transientErr) Error() string   { return "flaky" }
func (transientErr) Permanent() bool { return false }

func TestIsPermanent(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"plain error is retried", errors.New("boom"), false},
		{"permanent", permErr{}, true},
		{"permanent inside a wrap", fmt.Errorf("handling: %w", permErr{}), true},
		{"marked not permanent", transientErr{}, false},
	}
	for _, c := range cases {
		if got := isPermanent(c.err); got != c.want {
			t.Errorf("%s: isPermanent = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPrintable(t *testing.T) {
	if !printable([]byte(`{"a":1}`)) {
		t.Error("JSON text should be printable")
	}
	if printable([]byte{0, 0, 0, 0, 7, 2, 'x'}) {
		t.Error("Avro wire bytes (leading NULs) should not be printable")
	}
}

func TestDLQEnvelopeCarriesBinaryValuesIntact(t *testing.T) {
	raw := []byte{0, 0, 0, 0, 1, 0xff, 0xfe}
	out, err := json.Marshal(DeadLetter{OriginalTopic: "t", ValueBase64: raw})
	if err != nil {
		t.Fatal(err)
	}
	var back DeadLetter
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if string(back.ValueBase64) != string(raw) {
		t.Errorf("value changed in transit: %v", back.ValueBase64)
	}
}

func TestHandleWithRetryStopsRetryingOnceTheErrorIsPermanent(t *testing.T) {
	calls := 0
	err := handleWithRetry(context.Background(), func(context.Context, kafka.Message) error {
		calls++
		return permErr{}
	}, kafka.Message{}, 3, time.Millisecond, "t")
	if calls != 1 || err == nil {
		t.Errorf("calls = %d, err = %v; want one call and an error", calls, err)
	}
}

func TestHandleWithRetryRetriesTransientErrorsThenSucceeds(t *testing.T) {
	calls := 0
	err := handleWithRetry(context.Background(), func(context.Context, kafka.Message) error {
		calls++
		if calls < 3 {
			return errors.New("flaky")
		}
		return nil
	}, kafka.Message{}, 3, time.Millisecond, "t")
	if calls != 3 || err != nil {
		t.Errorf("calls = %d, err = %v; want 3 calls and success", calls, err)
	}
}

func TestHandleWithRetryGivesUpAfterMaxRetries(t *testing.T) {
	calls := 0
	err := handleWithRetry(context.Background(), func(context.Context, kafka.Message) error {
		calls++
		return errors.New("down")
	}, kafka.Message{}, 2, time.Millisecond, "t")
	if calls != 3 || err == nil {
		t.Errorf("calls = %d, err = %v; want 1 try + 2 retries, then the error", calls, err)
	}
}

func TestHandleWithRetryReturnsPromptlyWhenCancelledDuringTheRetryPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- handleWithRetry(ctx, func(context.Context, kafka.Message) error {
			cancel() // shutdown arrives while the first attempt is failing
			return errors.New("down")
		}, kafka.Message{}, 3, time.Hour, "t")
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still sleeping through the retry delay after cancel")
	}
}

func TestTopicSettingsComeFromTheConfigThenTheEnvironmentThenADefault(t *testing.T) {
	t.Setenv("KAFKA_REPLICATION_FACTOR", "3")
	t.Setenv("KAFKA_TOPIC_PARTITIONS", "")
	if got := firstPositive(0, envInt("KAFKA_REPLICATION_FACTOR"), 1); got != 3 {
		t.Errorf("replication factor from the environment = %d, want 3 (production creates retry topics replicated)", got)
	}
	if got := firstPositive(2, envInt("KAFKA_REPLICATION_FACTOR"), 1); got != 2 {
		t.Errorf("an explicit config value should win, got %d", got)
	}
	if got := firstPositive(0, envInt("KAFKA_TOPIC_PARTITIONS"), 3); got != 3 {
		t.Errorf("default partitions = %d, want 3", got)
	}
	t.Setenv("KAFKA_TOPIC_PARTITIONS", "not a number")
	if got := firstPositive(0, envInt("KAFKA_TOPIC_PARTITIONS"), 3); got != 3 {
		t.Errorf("an unparsable value should fall back to the default, got %d", got)
	}
}

// writerFake fails its first n writes, then accepts them.
type writerFake struct {
	failFirst int
	calls     int
	written   []kafka.Message
}

func (w *writerFake) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.calls++
	if w.calls <= w.failFirst {
		return errors.New("broker refused the write")
	}
	w.written = append(w.written, msgs...)
	return nil
}
func (w *writerFake) Close() error { return nil }

func testConsumer(w messageWriter) *consumer {
	return &consumer{
		cfg:   Config{GroupID: "test-group", RetryTiers: []time.Duration{time.Second}},
		label: "[test]",
		out:   w,
	}
}

func TestAMessageThatCannotBeRetriedOrDeadLetteredIsNotCountedAsHandled(t *testing.T) {
	dlqRetryPause = time.Millisecond
	t.Cleanup(func() { dlqRetryPause = 500 * time.Millisecond })
	w := &writerFake{failFirst: 1000} // refuses everything
	c := testConsumer(w)
	msg := kafka.Message{Topic: "order.created", Key: []byte("k"), Value: []byte("v")}
	before := outcomeFor("test-group", "order.created", "retried") + outcomeFor("test-group", "order.created", "dead_lettered")

	err := c.fail(context.Background(), msg, errors.New("boom"), 0)

	if err == nil {
		t.Fatal("fail reported success although neither topic took the message: the caller would commit it and lose it")
	}
	if after := outcomeFor("test-group", "order.created", "retried") + outcomeFor("test-group", "order.created", "dead_lettered"); after != before {
		t.Errorf("a message that went nowhere was counted as retried or dead-lettered")
	}
}

func TestAFailedMessageGoesToTheDeadLetterTopicWhenTheRetryTopicRefuses(t *testing.T) {
	dlqRetryPause = time.Millisecond
	t.Cleanup(func() { dlqRetryPause = 500 * time.Millisecond })
	w := &writerFake{failFirst: 1} // the retry topic write fails, the DLQ write after it works
	c := testConsumer(w)
	msg := kafka.Message{Topic: "trip.rated", Key: []byte("k"), Value: []byte("v")}

	if err := c.fail(context.Background(), msg, errors.New("boom"), 0); err != nil {
		t.Fatal(err)
	}
	if len(w.written) != 1 || w.written[0].Topic != events.DeadLetterTopic("trip.rated") {
		t.Errorf("wrote %v, want one message on the DLQ", w.written)
	}
}

func TestFailSafelyHoldsUntilTheMessageIsSafe(t *testing.T) {
	dlqRetryPause = time.Millisecond
	failBackoff = time.Millisecond
	t.Cleanup(func() { dlqRetryPause = 500 * time.Millisecond; failBackoff = time.Second })
	w := &writerFake{failFirst: 4} // the first few attempts at the retry and dead-letter topics all fail
	c := testConsumer(w)

	done := make(chan bool, 1)
	go func() {
		done <- c.failSafely(context.Background(), kafka.Message{Topic: "order.matched"}, errors.New("boom"), 0)
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("failSafely gave up")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failSafely never succeeded although the topics eventually accepted the message")
	}
	if len(w.written) != 1 {
		t.Errorf("%d messages written, want exactly 1 once it got through", len(w.written))
	}
}

func TestFailSafelyReleasesTheMessageOnShutdown(t *testing.T) {
	failBackoff = time.Hour // so it is certainly waiting when we cancel
	dlqRetryPause = time.Millisecond
	t.Cleanup(func() { failBackoff = time.Second; dlqRetryPause = 500 * time.Millisecond })
	c := testConsumer(&writerFake{failFirst: 1000})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool, 1)
	go func() { done <- c.failSafely(ctx, kafka.Message{Topic: "order.matched"}, errors.New("boom"), 0) }()
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Error("reported the message safe on shutdown: it would be committed and lost")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("still holding after the context was cancelled")
	}
}

func outcomeFor(group, topic, result string) float64 {
	return testutil.ToFloat64(messagesTotal.WithLabelValues(group, topic, result))
}
