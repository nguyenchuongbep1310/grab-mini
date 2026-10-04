// Package logging makes a service's logs machine-readable.
//
// Setup routes everything through log/slog, including the standard library's
// log.Printf the rest of the code still uses (slog.SetDefault redirects it),
// so one call at the top of main turns a service's output into one JSON object
// per line with the service name, which is what a log pipeline can index and
// query ("every line for orderId=...") instead of grepping free text.
//
//	LOG_FORMAT  json | text   default: json when APP_ENV=production, else text
//	LOG_LEVEL   debug | info | warn | error   default info
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Setup installs the default logger for service.
func Setup(service string) {
	format := strings.ToLower(os.Getenv("LOG_FORMAT"))
	if format == "" {
		format = "text"
		if strings.EqualFold(os.Getenv("APP_ENV"), "production") {
			format = "json"
		}
	}

	opts := &slog.HandlerOptions{Level: level(os.Getenv("LOG_LEVEL"))}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(traceHandler{h}).With("service", service))
}

// traceHandler adds trace_id and span_id to a record logged with a context
// that has a span (slog.InfoContext(ctx, ...)), so a log line can be matched
// to its trace and the other way round.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(a)}
}
func (h traceHandler) WithGroup(n string) slog.Handler { return traceHandler{h.Handler.WithGroup(n)} }

func level(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
