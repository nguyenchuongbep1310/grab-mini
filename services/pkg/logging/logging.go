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
	"log/slog"
	"os"
	"strings"
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
	slog.SetDefault(slog.New(h).With("service", service))
}

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
