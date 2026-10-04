// Command auth-service is a development stand-in for an identity provider.
//
// It signs a token for whoever asks ("I am rider-1"), with no password. That
// lets the apps and the other services exercise real token checking locally
// without building user accounts. It must not run in production: there, put
// a real identity provider in front and have the services verify its tokens.
// It refuses to start with APP_ENV=production unless ALLOW_DEV_TOKENS=true.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/secret"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

var tokensIssued = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "auth_tokens_issued_total",
	Help: "Development tokens issued, by role.",
}, []string{"role"})

// An id is what the services compare to ids in their data, so keep it plain.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type tokenRequest struct {
	Role auth.Role `json:"role"`
	ID   string    `json:"id"`
}

type tokenResponse struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func handleToken(issuer *auth.Issuer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
		var req tokenRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			auth.Deny(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if !req.Role.Valid() || !idPattern.MatchString(req.ID) {
			auth.Deny(w, http.StatusBadRequest, `role must be "rider" or "driver" and id letters, digits, - or _ (up to 64)`)
			return
		}
		token, exp, err := issuer.Issue(req.Role, req.ID)
		if err != nil {
			log.Printf("failed to issue token: %v", err)
			auth.Deny(w, http.StatusInternalServerError, "internal error")
			return
		}
		tokensIssued.WithLabelValues(string(req.Role)).Inc()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(tokenResponse{token, exp})
	}
}

func newMux(issuer *auth.Issuer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", handleToken(issuer))
	live := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
	// Nothing to depend on, so alive and ready are the same.
	mux.HandleFunc("GET /health", live)
	mux.HandleFunc("GET /livez", live)
	mux.HandleFunc("GET /readyz", live)
	mux.Handle("GET /metrics", metrics.Handler())
	return mux
}

func main() {
	logging.Setup("auth-service")
	env := loadEnv()
	if secret.Production() && !env.AllowDevTokens {
		log.Fatal("auth-service issues a token to anyone who asks and is for development only: " +
			"refusing to run with APP_ENV=production (set ALLOW_DEV_TOKENS=true to override, or use a real identity provider)")
	}
	issuer, err := auth.NewIssuer(env.JWTSecret, env.TokenTTL)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "auth-service")()

	log.Printf("auth-service listening on :%s (development token issuer, tokens last %s)", env.Port, env.TokenTTL)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("auth-service", metrics.HTTP(newMux(issuer)))); err != nil {
		log.Fatal(err)
	}
}
