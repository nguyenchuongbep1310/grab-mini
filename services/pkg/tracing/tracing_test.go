package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// recorder installs a tracer provider that keeps finished spans in memory.
func recorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { tp.Shutdown(context.Background()) })
	return exp
}

func TestAnHTTPRequestContinuesTheCallersTrace(t *testing.T) {
	exp := recorder(t)
	mux := http.NewServeMux()
	var seen trace.SpanContext
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		seen = trace.SpanContextFromContext(r.Context())
	})
	h := HTTP("order-service", mux)

	req := httptest.NewRequest(http.MethodGet, "/orders/123", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if seen.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("the handler ran in trace %s, want the caller's", seen.TraceID())
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("%d spans, want 1", len(spans))
	}
	if spans[0].Name != "GET /orders/{id}" {
		t.Errorf("span named %q; it should carry the route pattern, not the URL with an id", spans[0].Name)
	}
	if spans[0].Parent.SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("parent span %s, want the caller's", spans[0].Parent.SpanID())
	}
}

func TestProbesAndScrapesAreNotTraced(t *testing.T) {
	exp := recorder(t)
	h := HTTP("svc", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	for _, p := range []string{"/livez", "/readyz", "/metrics", "/health"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
	}
	if n := len(exp.GetSpans()); n != 0 {
		t.Errorf("%d spans for probes, want none", n)
	}
}

func TestAKafkaMessageCarriesTheTraceToItsConsumer(t *testing.T) {
	exp := recorder(t)
	ctx, producer := Tracer().Start(context.Background(), "POST /orders")

	// The publisher puts the context in the message headers (with another header already there)...
	msg := kafka.Message{Topic: "order.created", Partition: 2, Offset: 41,
		Headers: InjectKafka(ctx, []kafka.Header{{Key: "x-other", Value: []byte("kept")}})}
	producer.End()
	if string(msg.Headers[0].Value) != "kept" || len(msg.Headers) < 2 {
		t.Fatalf("headers = %v; existing ones must be kept and traceparent added", msg.Headers)
	}

	// ...and the consumer's span is its child, in the same trace.
	cctx, consume := StartConsume(context.Background(), "matching-service-group", msg)
	consume.End()

	if trace.SpanContextFromContext(cctx).TraceID() != producer.SpanContext().TraceID() {
		t.Error("the consumer started a new trace instead of continuing the publisher's")
	}
	var child tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.Name == "order.created process" {
			child = s
		}
	}
	if child.Parent.SpanID() != producer.SpanContext().SpanID() {
		t.Errorf("the consumer span's parent is %s, want the publisher's span", child.Parent.SpanID())
	}
	if child.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span kind %v, want consumer", child.SpanKind)
	}
	attrs := map[string]string{}
	for _, a := range child.Attributes {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	if attrs["messaging.destination.name"] != "order.created" || attrs["messaging.kafka.message.offset"] != "41" ||
		attrs["messaging.consumer.group.name"] != "matching-service-group" {
		t.Errorf("attributes %v", attrs)
	}
}

func TestAMessageWithNoTraceStartsANewOne(t *testing.T) {
	recorder(t)
	ctx, span := StartConsume(context.Background(), "g", kafka.Message{Topic: "t"})
	defer span.End()
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("a message without headers got no trace at all")
	}
}

func TestATraceSurvivesBeingStoredAsStrings(t *testing.T) {
	recorder(t)
	ctx, span := Tracer().Start(context.Background(), "request")
	defer span.End()

	stored := Map(ctx) // what the outbox keeps next to the row
	if stored["traceparent"] == "" {
		t.Fatalf("nothing stored: %v", stored)
	}
	back := trace.SpanContextFromContext(FromMap(context.Background(), stored))
	if back.TraceID() != span.SpanContext().TraceID() || back.SpanID() != span.SpanContext().SpanID() {
		t.Error("the restored context isn't the original span")
	}
}

func TestSpansAreExportedOverOTLPWhenAnEndpointIsSet(t *testing.T) {
	var posts atomic.Int32
	var path atomic.Value
	var contentType atomic.Value
	var body atomic.Int64
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		posts.Add(1)
		path.Store(r.URL.Path)
		contentType.Store(r.Header.Get("Content-Type"))
		body.Store(int64(len(b)))
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)

	stop := Setup(context.Background(), "test-service")
	_, span := Tracer().Start(context.Background(), "an operation")
	span.End()
	stop() // flushes the batch

	if posts.Load() == 0 {
		t.Fatal("nothing reached the collector")
	}
	if p, _ := path.Load().(string); p != "/v1/traces" {
		t.Errorf("posted to %q, want /v1/traces", p)
	}
	if ct, _ := contentType.Load().(string); ct != "application/x-protobuf" {
		t.Errorf("content type %q", ct)
	}
	if body.Load() == 0 {
		t.Error("an empty body")
	}
}

func TestNothingIsExportedWithoutAnEndpointButTracesStillPropagate(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	stop := Setup(context.Background(), "svc")
	defer stop()

	ctx, span := Tracer().Start(context.Background(), "x")
	defer span.End()
	if len(Map(ctx)["traceparent"]) == 0 {
		t.Error("no traceparent without an exporter: a trace could not pass through this service")
	}
}

func TestSamplingRatio(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	t.Setenv("APP_ENV", "")
	if got := sampleRatio(); got != 1 {
		t.Errorf("development ratio %v, want 1", got)
	}
	t.Setenv("APP_ENV", "production")
	if got := sampleRatio(); got != 0.1 {
		t.Errorf("production ratio %v, want 0.1", got)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.5")
	if got := sampleRatio(); got != 0.5 {
		t.Errorf("explicit ratio %v, want 0.5", got)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "7")
	if got := sampleRatio(); got != 0.1 {
		t.Errorf("an invalid ratio should fall back, got %v", got)
	}
}
