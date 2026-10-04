package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakes returns an auth server and a gateway; the gateway rejects the first
// token it is shown (as if it had expired) when rejectFirst is set.
func fakes(t *testing.T, rejectFirst bool) (c *client, tokensIssued, reports *atomic.Int32) {
	t.Helper()
	tokensIssued, reports = new(atomic.Int32), new(atomic.Int32)

	authSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Role, ID string }
		json.NewDecoder(r.Body).Decode(&req)
		if req.Role != "driver" || req.ID != "driver-1" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		n := tokensIssued.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"token": "tok" + string(rune('0'+n)), "expiresAt": time.Now().Add(time.Hour)})
	}))
	t.Cleanup(authSrv.Close)

	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rejectFirst && r.Header.Get("Authorization") == "Bearer tok1" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["lat"] == nil || body["lng"] == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reports.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(gw.Close)

	return &client{http: http.DefaultClient, authURL: authSrv.URL, gatewayURL: gw.URL, tokens: map[string]token{}}, tokensIssued, reports
}

func TestTheTokenIsFetchedOnceAndReused(t *testing.T) {
	c, issued, reports := fakes(t, false)
	for i := 0; i < 3; i++ {
		if err := c.report(context.Background(), simDriver{ID: "driver-1", Lat: 10, Lng: 106}); err != nil {
			t.Fatal(err)
		}
	}
	if issued.Load() != 1 || reports.Load() != 3 {
		t.Errorf("%d tokens, %d reports; want 1 token reused for 3 reports", issued.Load(), reports.Load())
	}
}

func TestARejectedTokenIsReplacedOnce(t *testing.T) {
	c, issued, reports := fakes(t, true)
	if err := c.report(context.Background(), simDriver{ID: "driver-1", Lat: 10, Lng: 106}); err != nil {
		t.Fatal(err)
	}
	if issued.Load() != 2 || reports.Load() != 1 {
		t.Errorf("%d tokens, %d reports; want a second token after the 401", issued.Load(), reports.Load())
	}
}

func TestAnUnknownDriverFailsInsteadOfLooping(t *testing.T) {
	c, issued, _ := fakes(t, false)
	if err := c.report(context.Background(), simDriver{ID: "nobody"}); err == nil {
		t.Error("report succeeded without a token")
	}
	if issued.Load() != 0 {
		t.Errorf("%d tokens issued", issued.Load())
	}
}
