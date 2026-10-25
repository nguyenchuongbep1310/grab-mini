// Package serve runs an HTTP server that stops cleanly when its context is
// cancelled (SIGTERM from Kubernetes or Docker, Ctrl-C locally), so a deploy
// doesn't cut requests off halfway.
package serve

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Grace is how long in-flight requests get to finish after ctx is cancelled.
const Grace = 10 * time.Second

// Limits on a client's connection. Without them one that sends its request a
// byte at a time (or never finishes it) holds a connection and a goroutine
// for as long as it likes. Every response in these services is small and
// quick; a WebSocket leaves the server's control when it is upgraded, so the
// deadlines do not cut one short.
const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 15 * time.Second
	WriteTimeout      = 30 * time.Second
	IdleTimeout       = 2 * time.Minute
	MaxHeaderBytes    = 32 << 10
)

// MaxBodyBytes is the most request body Run lets any handler read. These APIs
// take small JSON objects; a handler that wants less (the location gateway
// takes 1 KiB) sets its own, tighter limit.
const MaxBodyBytes = 64 << 10

// LimitBody wraps h so no request body reads past n bytes: the read fails
// with *http.MaxBytesError, which a JSON decoder reports as a bad body.
func LimitBody(h http.Handler, n int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, n)
		h.ServeHTTP(w, r)
	})
}

// Server returns the http.Server Run uses, with the limits above.
func Server(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
}

// Run serves h on addr until ctx is cancelled, then waits up to Grace for
// open requests. It returns nil after a clean shutdown, or the listen error.
func Run(ctx context.Context, addr string, h http.Handler) error {
	srv := Server(addr, LimitBody(h, MaxBodyBytes))

	done := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), Grace)
		defer cancel()
		done <- srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return <-done
}
