package auth

import (
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// Limiter is a token bucket per caller: each may make burst requests at once
// and then perSecond more every second. A token proves who is calling but not
// that they are calling sensibly; without a limit, one rider's script can run
// the pricing, order and rating paths (and the database behind them) as hard
// as it likes.
//
// It is in memory, so with several instances a caller's real allowance is
// the limit times the number of instances: a brake on abuse, not an exact quota.
type Limiter struct {
	rate  float64 // tokens added per second
	burst float64
	now   func() time.Time

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens float64
	at     time.Time
}

// sweepAfter and sweepOver bound the map: once it holds more than sweepOver
// callers, buckets that have refilled completely (callers gone quiet) are
// dropped, at most once per sweepAfter.
const (
	sweepAfter = time.Minute
	sweepOver  = 1024
)

// NewLimiter allows burst requests at once per caller, refilling at perSecond.
func NewLimiter(perSecond float64, burst int) *Limiter {
	return &Limiter{rate: perSecond, burst: float64(burst), now: time.Now, buckets: map[string]*bucket{}}
}

// Allow takes a token for key. When none is left it reports how long until
// one is.
// RateLimitScaleEnv scales every limit built by LimiterFromEnv: 2 allows twice
// the rate and burst, 0 turns limiting off. Load tests drive a few simulated
// users far harder than a person would, and set it to 0.
const RateLimitScaleEnv = "RATE_LIMIT_SCALE"

// LimiterFromEnv is NewLimiter with the limits scaled by RATE_LIMIT_SCALE. It
// returns nil when the scale is 0, and a nil *Limiter lets everything through.
func LimiterFromEnv(perSecond float64, burst int) *Limiter {
	scale := 1.0
	if v := os.Getenv(RateLimitScaleEnv); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 {
			log.Fatalf("%s: %q is not a number of 0 or more", RateLimitScaleEnv, v)
		}
		scale = f
	}
	if scale == 0 {
		return nil
	}
	return NewLimiter(perSecond*scale, max(1, int(math.Round(float64(burst)*scale))))
}

func (l *Limiter) Allow(key string) (ok bool, retryAfter time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.sweep(now)

	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, at: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.at).Seconds()*l.rate)
	b.at = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}

func (l *Limiter) sweep(now time.Time) {
	if len(l.buckets) <= sweepOver || now.Sub(l.lastSweep) < sweepAfter {
		return
	}
	l.lastSweep = now
	for k, b := range l.buckets {
		if b.tokens+now.Sub(b.at).Seconds()*l.rate >= l.burst {
			delete(l.buckets, k)
		}
	}
}

// Wrap limits h per caller, answering 429 with a Retry-After once a caller is
// over. It reads who is calling from the context, so it goes inside Require:
//
//	verifier.Require(limiter.Wrap(h), auth.Rider)
//
// A request with no verified caller is let through to h, which refuses it.
func (l *Limiter) Wrap(h http.Handler) http.Handler {
	if l == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, ok := From(r.Context()); ok {
			if allowed, wait := l.Allow(string(id.Role) + ":" + id.Subject); !allowed {
				w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(wait.Seconds())))))
				Deny(w, http.StatusTooManyRequests, "too many requests, slow down")
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}
