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

// Run serves h on addr until ctx is cancelled, then waits up to Grace for
// open requests. It returns nil after a clean shutdown, or the listen error.
func Run(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h}

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
