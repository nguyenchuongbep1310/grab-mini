// Package tracing follows one request across services, so "why was this
// order slow" has an answer: a single trace from the HTTP call that created the
// order, through the outbox and each Kafka hop, to the match and the trip.
//
// A trace id travels in the W3C traceparent format: in the header of an HTTP
// request, and in the header of a Kafka message. Services always propagate it
// (cheap, and what lets a trace start in one service and continue in the next)
// and, if OTEL_EXPORTER_OTLP_ENDPOINT is set, also send their spans to a
// collector over OTLP/HTTP.
//
//	OTEL_EXPORTER_OTLP_ENDPOINT   e.g. http://otel-collector:4318 (unset: don't export)
//	OTEL_TRACES_SAMPLER_ARG       fraction of new traces to record, 0 to 1
//	                              (default 1 in development, 0.1 in production);
//	                              a trace already sampled upstream stays sampled
package tracing

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup installs the global tracer provider and propagator for service and
// returns a function that flushes pending spans and stops it (defer it in main).
func Setup(ctx context.Context, service string) (stop func()) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio()))),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		// The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT itself.
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			log.Printf("tracing: could not create the OTLP exporter, spans won't be exported: %v", err)
		} else {
			opts = append(opts, sdktrace.WithBatcher(exp))
		}
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("tracing: could not flush spans on shutdown: %v", err)
		}
	}
}

func sampleRatio() float64 {
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			return f
		}
	}
	if strings.EqualFold(os.Getenv("APP_ENV"), "production") {
		return 0.1
	}
	return 1
}

// Tracer is the tracer every package here uses.
func Tracer() trace.Tracer { return otel.Tracer("grab-mini") }

// HTTP traces every request h serves, continuing the trace the caller sent in
// traceparent (if any). A span is named by the route pattern the mux matched
// ("POST /orders"), not the URL, which would give every order id its own name.
func HTTP(service string, h http.Handler) http.Handler {
	named := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
		if r.Pattern != "" {
			trace.SpanFromContext(r.Context()).SetName(r.Pattern)
		}
	})
	return otelhttp.NewHandler(named, service,
		otelhttp.WithFilter(func(r *http.Request) bool {
			switch r.URL.Path { // probes and scrapes aren't worth a span each
			case "/metrics", "/livez", "/readyz", "/health":
				return false
			}
			return true
		}),
	)
}

// kafkaCarrier adapts a message's headers to the propagator.
type kafkaCarrier struct{ headers *[]kafka.Header }

func (c kafkaCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if strings.EqualFold(h.Key, key) {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectKafka returns headers carrying ctx's trace, to put on a message, added
// to existing (which may be nil).
func InjectKafka(ctx context.Context, existing []kafka.Header) []kafka.Header {
	hs := append([]kafka.Header(nil), existing...)
	otel.GetTextMapPropagator().Inject(ctx, kafkaCarrier{&hs})
	return hs
}

// ExtractKafka returns ctx continuing the trace in msg's headers, if it has one.
func ExtractKafka(ctx context.Context, msg kafka.Message) context.Context {
	hs := msg.Headers
	return otel.GetTextMapPropagator().Extract(ctx, kafkaCarrier{&hs})
}

// StartConsume starts the span for handling msg, a child of the span that
// published it. End it when the handler returns.
func StartConsume(ctx context.Context, group string, msg kafka.Message) (context.Context, trace.Span) {
	ctx = ExtractKafka(ctx, msg)
	return Tracer().Start(ctx, msg.Topic+" process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.consumer.group.name", group),
			attribute.Int("messaging.destination.partition.id", msg.Partition),
			attribute.Int64("messaging.kafka.message.offset", msg.Offset),
		))
}

// SpanContextOf is the span context ctx carries (invalid if none).
func SpanContextOf(ctx context.Context) trace.SpanContext { return trace.SpanContextFromContext(ctx) }

// Map and FromMap turn a trace context into plain strings and back, for storing
// next to a row (the outbox) so the trace survives the wait for the relay.
func Map(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func FromMap(ctx context.Context, m map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m))
}

// flushTimeout bounds how long shutdown waits for spans to be sent.
const flushTimeout = 5 * time.Second
