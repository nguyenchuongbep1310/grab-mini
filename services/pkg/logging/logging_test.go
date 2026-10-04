package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// capture runs Setup and returns what a log.Printf and an slog call write.
func capture(t *testing.T, env map[string]string) string {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	r, w, _ := os.Pipe()
	orig := os.Stderr
	os.Stderr = w
	Setup("trip-service")
	os.Stderr = orig // the handler already holds w

	log.Printf("plain printf line")
	slog.Info("structured line", "orderId", "o-1")
	w.Close()

	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String()
}

func TestProductionLogsJSONIncludingStandardLogCalls(t *testing.T) {
	out := capture(t, map[string]string{"APP_ENV": "production", "LOG_FORMAT": "", "LOG_LEVEL": ""})

	var lines []map[string]any
	for _, l := range bytes.Split(bytes.TrimSpace([]byte(out)), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(l, &m); err != nil {
			t.Fatalf("not JSON: %q", l)
		}
		lines = append(lines, m)
	}
	if len(lines) != 2 {
		t.Fatalf("%d lines, want 2:\n%s", len(lines), out)
	}
	for _, m := range lines {
		if m["service"] != "trip-service" || m["level"] != "INFO" {
			t.Errorf("line %v lacks the service or level", m)
		}
	}
	if lines[0]["msg"] != "plain printf line" {
		t.Errorf("a log.Printf call became %v", lines[0])
	}
	if lines[1]["orderId"] != "o-1" {
		t.Errorf("an attribute was lost: %v", lines[1])
	}
}

func TestDevelopmentLogsText(t *testing.T) {
	out := capture(t, map[string]string{"APP_ENV": "", "LOG_FORMAT": "", "LOG_LEVEL": ""})
	if json.Valid([]byte(out)) || !bytes.Contains([]byte(out), []byte("service=trip-service")) {
		t.Errorf("want key=value text, got:\n%s", out)
	}
}

func TestLevelFiltersAndFormatCanBeForced(t *testing.T) {
	out := capture(t, map[string]string{"APP_ENV": "", "LOG_FORMAT": "json", "LOG_LEVEL": "error"})
	if out != "" {
		t.Errorf("info lines were written at level error:\n%s", out)
	}
}

func TestALogLineInsideASpanCarriesItsTraceId(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_FORMAT", "")
	t.Setenv("LOG_LEVEL", "")
	r, w, _ := os.Pipe()
	orig := os.Stderr
	os.Stderr = w
	Setup("svc")
	os.Stderr = orig

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	slog.InfoContext(ctx, "inside a span")
	slog.Info("outside any span")
	w.Close()

	var buf bytes.Buffer
	buf.ReadFrom(r)
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var first, second map[string]any
	json.Unmarshal(lines[0], &first)
	json.Unmarshal(lines[1], &second)
	if first["trace_id"] != "0102030405060708090a0b0c0d0e0f10" || first["span_id"] != "0102030405060708" {
		t.Errorf("the line inside the span: %v", first)
	}
	if _, has := second["trace_id"]; has {
		t.Errorf("a line outside any span got a trace id: %v", second)
	}
}
