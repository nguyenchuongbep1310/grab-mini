package kafkaconsumer

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"grab-mini/pkg/outbox"
	"grab-mini/pkg/tracing"
)

// One request, followed all the way: it saves an event in the outbox, the relay
// publishes it to a real broker, a consumer handles it (failing, so it goes
// through a retry topic), and every consumer span is in the request's trace as a
// child of the request span. Needs TEST_DATABASE_URL and TEST_KAFKA_BROKERS.
func TestOneRequestIsOneTraceThroughTheOutboxKafkaAndARetry(t *testing.T) {
	b := brokers(t)
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { tp.Shutdown(ctx) })

	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := outbox.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	db.Exec(ctx, `DELETE FROM outbox WHERE source = 'trace-test'`)

	topic := newTopic(t, b, 1)

	// The consumer: fail the first two attempts (the second is from the first retry topic).
	cfg := config(b, topic)
	var mu sync.Mutex
	var seen []trace.SpanContext
	done := make(chan struct{})
	start(t, cfg, func(ctx context.Context, msg kafka.Message) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, trace.SpanContextFromContext(ctx))
		switch len(seen) {
		case 1, 2, 3: // in place twice (MaxRetries is 1 with tiers), then from retry tier 1
			return errors.New("not yet")
		case 4:
			close(done)
		}
		return nil
	})

	// The request: queue the event under its span, then the relay publishes it.
	reqCtx, request := tracing.Tracer().Start(ctx, "POST /orders")
	if err := outbox.Add(reqCtx, db, "trace-test", topic, []byte("order-1"), []byte("created")); err != nil {
		t.Fatal(err)
	}
	request.End()

	w := &kafka.Writer{Addr: kafka.TCP(b...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
	defer w.Close()
	if n, err := outbox.NewRelay(db, w, "trace-test").Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v", n, err)
	}

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the message never got through its retries")
	}

	want := request.SpanContext()
	mu.Lock()
	defer mu.Unlock()
	for i, sc := range seen {
		if sc.TraceID() != want.TraceID() {
			t.Errorf("attempt %d ran in trace %s, want the request's %s", i+1, sc.TraceID(), want.TraceID())
		}
	}

	var consumers int
	for _, s := range exp.GetSpans() {
		if s.SpanKind != trace.SpanKindConsumer {
			continue
		}
		consumers++
		if s.Parent.SpanID() != want.SpanID() {
			t.Errorf("consumer span %q has parent %s, want the request span %s", s.Name, s.Parent.SpanID(), want.SpanID())
		}
	}
	// One span for the source consumer's attempts (it retries in place inside it)
	// and one per retry-topic delivery that happened.
	if consumers < 2 {
		t.Errorf("%d consumer spans, want one for the source and one for the retry topic delivery", consumers)
	}
}
