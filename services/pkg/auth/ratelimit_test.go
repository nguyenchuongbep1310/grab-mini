package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testLimiter(perSecond float64, burst int) (*Limiter, *time.Time) {
	now := time.Unix(1_000_000, 0)
	l := NewLimiter(perSecond, burst)
	l.now = func() time.Time { return now }
	return l, &now
}

func TestALimiterAllowsABurstThenRefuses(t *testing.T) {
	l, _ := testLimiter(1, 3)
	for i := 0; i < 3; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("request %d of the burst was refused", i+1)
		}
	}
	ok, wait := l.Allow("a")
	if ok {
		t.Fatal("a fourth request was allowed")
	}
	if wait <= 0 || wait > time.Second {
		t.Errorf("retry after %v, want within (0, 1s]", wait)
	}
}

func TestALimiterRefillsOverTimeButNeverPastTheBurst(t *testing.T) {
	l, now := testLimiter(1, 2)
	l.Allow("a")
	l.Allow("a")
	*now = now.Add(time.Second)
	if ok, _ := l.Allow("a"); !ok {
		t.Error("a token was not refilled after a second")
	}
	if ok, _ := l.Allow("a"); ok {
		t.Error("more than one token refilled in a second")
	}
	*now = now.Add(time.Hour)
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("request %d after a long quiet spell was refused", i+1)
		}
	}
	if ok, _ := l.Allow("a"); ok {
		t.Error("a long quiet spell banked more than the burst")
	}
}

func TestCallersHaveSeparateBuckets(t *testing.T) {
	l, _ := testLimiter(1, 1)
	l.Allow("a")
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("a was not limited")
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Error("b was limited by a's requests")
	}
}

func TestQuietCallersAreForgotten(t *testing.T) {
	l, now := testLimiter(1, 1)
	for i := 0; i <= sweepOver+10; i++ {
		l.Allow(string(rune('a'+i%26)) + time.Duration(i).String())
	}
	*now = now.Add(2 * sweepAfter)
	l.Allow("new")
	if n := len(l.buckets); n > 2 {
		t.Errorf("%d buckets kept after everyone went quiet, want the sweep to drop them", n)
	}
}

func TestWrapAnswers429WithRetryAfterPerCaller(t *testing.T) {
	l, _ := testLimiter(1, 1)
	called := 0
	h := l.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called++ }))
	call := func(subject string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/", nil)
		r = r.WithContext(With(r.Context(), Identity{Subject: subject, Role: Rider}))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := call("rider-1"); w.Code != 200 {
		t.Fatalf("first request = %d", w.Code)
	}
	w := call("rider-1")
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Errorf("second request = %d, Retry-After %q; want 429 with a Retry-After", w.Code, w.Header().Get("Retry-After"))
	}
	if w := call("rider-2"); w.Code != 200 {
		t.Errorf("another rider got %d", w.Code)
	}
	if called != 2 {
		t.Errorf("handler ran %d times, want 2", called)
	}
}

func TestLimiterFromEnvScalesOrDisables(t *testing.T) {
	t.Setenv(RateLimitScaleEnv, "")
	if l := LimiterFromEnv(1, 4); l.burst != 4 || l.rate != 1 {
		t.Errorf("default limiter = rate %v burst %v", l.rate, l.burst)
	}
	t.Setenv(RateLimitScaleEnv, "2")
	if l := LimiterFromEnv(1, 4); l.burst != 8 || l.rate != 2 {
		t.Errorf("scaled limiter = rate %v burst %v, want 2 and 8", l.rate, l.burst)
	}
	t.Setenv(RateLimitScaleEnv, "0")
	l := LimiterFromEnv(1, 4)
	if l != nil {
		t.Fatal("scale 0 should disable the limiter")
	}
	for i := 0; i < 100; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatal("a disabled limiter refused a request")
		}
	}
	called := false
	l.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if !called {
		t.Error("a disabled limiter's Wrap did not call the handler")
	}
}
