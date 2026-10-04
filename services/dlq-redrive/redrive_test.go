package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// Against a real broker: set TEST_KAFKA_BROKERS.
func brokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	return []string{b}
}

// topics makes a topic and its dead-letter topic, one partition each.
func topics(t *testing.T, b []string) string {
	t.Helper()
	topic := "rd-" + uuid.NewString()[:8]
	client := &kafka.Client{Addr: kafka.TCP(b...)}
	resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: events.DeadLetterTopic(topic), NumPartitions: 1, ReplicationFactor: 1},
	}})
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

// deadLetter puts a dead letter on topic's DLQ, as the consumer loop would.
func deadLetter(t *testing.T, b []string, topic, key string, value []byte, errText string) {
	t.Helper()
	payload, _ := json.Marshal(kafkaconsumer.DeadLetter{OriginalTopic: topic, Error: errText, ValueBase64: value})
	w := &kafka.Writer{Addr: kafka.TCP(b...), Topic: events.DeadLetterTopic(topic), RequiredAcks: kafka.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		t.Fatal(err)
	}
}

func raw(t *testing.T, b []string, topic string, key, value string) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(b...), Topic: events.DeadLetterTopic(topic), RequiredAcks: kafka.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(value)}); err != nil {
		t.Fatal(err)
	}
}

// readAll returns what is on topic right now.
func readAll(t *testing.T, b []string, topic string) []kafka.Message {
	t.Helper()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b, Topic: topic, Partition: 0, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	r.SetOffset(kafka.FirstOffset)
	var out []kafka.Message
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		m, err := r.ReadMessage(ctx)
		cancel()
		if err != nil {
			return out
		}
		out = append(out, m)
	}
}

func cfg(b []string, topic string) Config {
	return Config{Brokers: b, Topic: topic, Group: "redrive-" + uuid.NewString()[:8], Idle: 2 * time.Second}
}

func TestDeadLettersGoBackToTheOriginalTopicWithTheirKeyAndExactBytes(t *testing.T) {
	b := brokers(t)
	topic := topics(t, b)
	avro := []byte{0, 0, 0, 0, 7, 0xff, 0xfe, 'x'} // not text: it must survive the base64 round trip
	deadLetter(t, b, topic, "order-1", avro, "database down")
	deadLetter(t, b, topic, "order-2", []byte("second"), "database down")

	res, err := Run(context.Background(), cfg(b, topic))
	if err != nil || res.Redriven != 2 {
		t.Fatalf("Run = %+v, %v; want 2 redriven", res, err)
	}

	got := readAll(t, b, topic)
	if len(got) != 2 || string(got[0].Key) != "order-1" || !bytes.Equal(got[0].Value, avro) || string(got[1].Value) != "second" {
		t.Fatalf("original topic holds %v", got)
	}
	var from string
	for _, h := range got[0].Headers {
		if h.Key == headerRedrivenFrom {
			from = string(h.Value)
		}
	}
	if from != events.DeadLetterTopic(topic) {
		t.Errorf("x-redriven-from = %q, want the DLQ", from)
	}
}

func TestRunningItAgainDoesNotRedriveWhatItAlreadySent(t *testing.T) {
	b := brokers(t)
	topic := topics(t, b)
	deadLetter(t, b, topic, "k", []byte("once"), "boom")
	c := cfg(b, topic)

	if res, _ := Run(context.Background(), c); res.Redriven != 1 {
		t.Fatalf("first run redriven %d", res.Redriven)
	}
	if res, err := Run(context.Background(), c); err != nil || res.Redriven != 0 {
		t.Fatalf("second run = %+v, %v; want nothing left", res, err)
	}
	if n := len(readAll(t, b, topic)); n != 1 {
		t.Errorf("%d messages on the original topic, want exactly 1", n)
	}

	// New dead letters after that are picked up from where it left off.
	deadLetter(t, b, topic, "k2", []byte("later"), "boom")
	if res, _ := Run(context.Background(), c); res.Redriven != 1 {
		t.Errorf("third run redriven %d, want just the new one", res.Redriven)
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	b := brokers(t)
	topic := topics(t, b)
	deadLetter(t, b, topic, "k", []byte("v"), "boom")
	c := cfg(b, topic)

	c.DryRun = true
	if res, err := Run(context.Background(), c); err != nil || res.Redriven != 1 {
		t.Fatalf("dry run = %+v, %v; want it to report 1", res, err)
	}
	if n := len(readAll(t, b, topic)); n != 0 {
		t.Fatalf("a dry run published %d message(s)", n)
	}

	c.DryRun = false // and it still hasn't moved the position: a real run sees the message
	if res, _ := Run(context.Background(), c); res.Redriven != 1 {
		t.Errorf("the real run after a dry run redriven %d, want 1", res.Redriven)
	}
}

func TestAnErrorFilterRedrivesOnlyThatCause(t *testing.T) {
	b := brokers(t)
	topic := topics(t, b)
	deadLetter(t, b, topic, "a", []byte("a"), "connection refused: postgres")
	deadLetter(t, b, topic, "b", []byte("b"), "schema registry 409")
	deadLetter(t, b, topic, "c", []byte("c"), "connection refused: redis")

	c := cfg(b, topic)
	c.ErrorContains = "connection refused"
	res, err := Run(context.Background(), c)
	if err != nil || res.Redriven != 2 || res.Skipped != 1 {
		t.Fatalf("Run = %+v, %v; want 2 redriven, 1 skipped", res, err)
	}
	got := readAll(t, b, topic)
	if len(got) != 2 || string(got[0].Key) != "a" || string(got[1].Key) != "c" {
		t.Errorf("redriven keys %v, want a and c", got)
	}
}

func TestMaxLimitsAndTheNextRunContinues(t *testing.T) {
	b := brokers(t)
	topic := topics(t, b)
	for _, k := range []string{"1", "2", "3"} {
		deadLetter(t, b, topic, k, []byte(k), "boom")
	}
	c := cfg(b, topic)
	c.Max = 2

	if res, _ := Run(context.Background(), c); res.Redriven != 2 {
		t.Fatalf("redriven %d, want 2", res.Redriven)
	}
	c.Max = 0
	if res, _ := Run(context.Background(), c); res.Redriven != 1 {
		t.Errorf("the next run redriven %d, want the remaining 1", res.Redriven)
	}
	if n := len(readAll(t, b, topic)); n != 3 {
		t.Errorf("%d messages on the original topic, want 3", n)
	}
}

func TestSomethingThatIsNotADeadLetterIsSkippedNotRedriven(t *testing.T) {
	b := brokers(t)
	topic := topics(t, b)
	raw(t, b, topic, "x", "this is not JSON")
	raw(t, b, topic, "y", `{"error":"no value"}`)
	deadLetter(t, b, topic, "ok", []byte("fine"), "boom")

	res, err := Run(context.Background(), cfg(b, topic))
	if err != nil || res.Redriven != 1 || res.Skipped != 2 {
		t.Fatalf("Run = %+v, %v; want 1 redriven and 2 skipped", res, err)
	}
	if got := readAll(t, b, topic); len(got) != 1 || string(got[0].Key) != "ok" {
		t.Errorf("original topic holds %v", got)
	}
}

func TestItRefusesTheDLQTopicName(t *testing.T) {
	if _, err := Run(context.Background(), Config{Topic: "order.created.DLQ"}); err == nil {
		t.Error("accepted a .DLQ topic: redriving a DLQ into itself would loop")
	}
	if _, err := Run(context.Background(), Config{}); err == nil {
		t.Error("accepted no topic")
	}
}

func TestGroupNameFromAFilter(t *testing.T) {
	for in, want := range map[string]string{"connection refused": "connection-refused", "  No such column: x!": "no-such-column-x", "409": "409"} {
		if got := slug(in); got != want {
			t.Errorf("slug(%q) = %q, want %q", in, got, want)
		}
	}
}
