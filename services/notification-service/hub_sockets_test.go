package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

// hubServer serves /<kind>/<id> sockets through a hub whose Redis may be
// unreachable (register logs the failed subscribe and still tracks the socket).
func hubServer(t *testing.T, h *hub, ctx context.Context) string {
	t.Helper()
	up := newUpgrader(nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind, id, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		serveWS(up, w, r,
			func(ws *websocket.Conn) *conn { return h.register(ctx, kind, id, ws) },
			func(c *conn) { h.unregister(ctx, kind, id, c) })
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func offlineHub(t *testing.T) (*hub, context.Context) {
	t.Helper()
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	return newHub(ctx, rdb), ctx
}

func (h *hub) count(kind, id string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns[connKey{kind, id}])
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dialN(t *testing.T, h *hub, url, kind, id string, want int) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(url+"/"+kind+"/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	waitFor(t, "socket to register", func() bool { return h.count(kind, id) == want })
	return ws
}

func readText(t *testing.T, ws *websocket.Conn) string {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, b, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestTwoSocketsOfOnePersonBothReceiveAPush(t *testing.T) {
	h, ctx := offlineHub(t)
	url := hubServer(t, h, ctx)
	a := dialN(t, h, url, kindRider, "r1", 1)
	b := dialN(t, h, url, kindRider, "r1", 2)

	h.deliver(channelFor(kindRider, "r1"), []byte("hello"))
	if got := readText(t, a); got != "hello" {
		t.Errorf("first socket got %q", got)
	}
	if got := readText(t, b); got != "hello" {
		t.Errorf("second socket got %q", got)
	}
}

func TestTheSixthSocketEvictsTheOldest(t *testing.T) {
	h, ctx := offlineHub(t)
	url := hubServer(t, h, ctx)
	before := testutil.ToFloat64(socketsEvicted.WithLabelValues(kindDriver))
	var socks []*websocket.Conn
	for i := 1; i <= 5; i++ {
		socks = append(socks, dialN(t, h, url, kindDriver, "d1", i))
	}
	sixth, _, err := websocket.DefaultDialer.Dial(url+"/"+kindDriver+"/d1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sixth.Close()

	socks[0].SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := socks[0].ReadMessage(); err == nil {
		t.Fatal("the oldest socket delivered a message instead of being closed")
	} else if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
		t.Fatal("the oldest socket was left open")
	}
	waitFor(t, "five sockets", func() bool { return h.count(kindDriver, "d1") == 5 })
	if d := testutil.ToFloat64(socketsEvicted.WithLabelValues(kindDriver)) - before; d != 1 {
		t.Errorf("evicted counter moved by %v, want 1", d)
	}
	// The others stay up and still receive.
	h.deliver(channelFor(kindDriver, "d1"), []byte("x"))
	if got := readText(t, socks[1]); got != "x" {
		t.Errorf("got %q", got)
	}
	if got := readText(t, sixth); got != "x" {
		t.Errorf("got %q", got)
	}
}

func TestAStuckSocketDoesNotDelayTheOther(t *testing.T) {
	h, ctx := offlineHub(t)
	url := hubServer(t, h, ctx)
	stuck := dialN(t, h, url, kindRider, "r2", 1) // never read
	_ = stuck
	live := dialN(t, h, url, kindRider, "r2", 2)

	// Drain the live one continuously while flooding: the stuck one's buffers
	// fill and its writer blocks (writeWait is 10s), but the live one keeps up.
	got := make(chan struct{}, 1024)
	go func() {
		for {
			if _, _, err := live.ReadMessage(); err != nil {
				return
			}
			got <- struct{}{}
		}
	}()
	big := []byte(strings.Repeat("x", 256<<10))
	for i := 0; i < 80; i++ {
		h.deliver(channelFor(kindRider, "r2"), big)
		time.Sleep(2 * time.Millisecond)
	}
	start := time.Now()
	h.deliver(channelFor(kindRider, "r2"), []byte("small"))
	// deliver itself must not block on the stuck socket.
	if d := time.Since(start); d > time.Second {
		t.Errorf("deliver took %v with a stuck socket", d)
	}
	n := 0
	deadline := time.After(5 * time.Second)
	for n < 81 {
		select {
		case <-got:
			n++
		case <-deadline:
			t.Fatalf("live socket got %d of 81 messages while the other was stuck", n)
		}
	}
}

func TestConcurrentConnectAndDisconnect(t *testing.T) {
	h, ctx := offlineHub(t)
	url := hubServer(t, h, ctx)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			id := fmt.Sprintf("p%d", g%3)
			for i := 0; i < 15; i++ {
				ws, _, err := websocket.DefaultDialer.Dial(url+"/"+kindRider+"/"+id, nil)
				if err != nil {
					t.Error(err)
					return
				}
				h.deliver(channelFor(kindRider, id), []byte("m"))
				ws.Close()
			}
		}(g)
	}
	wg.Wait()
	waitFor(t, "all sockets forgotten", func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return len(h.conns) == 0
	})
}

func subscribed(t *testing.T, rdb *redis.Client, ch string) bool {
	t.Helper()
	l, err := rdb.PubSubChannels(context.Background(), "notify:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range l {
		if c == ch {
			return true
		}
	}
	return false
}

func TestSubscriptionLivesUntilTheLastSocketCloses(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	h := newHub(ctx, rdb)
	go h.run(ctx)
	url := hubServer(t, h, ctx)
	ch := channelFor(kindRider, "r3")

	a := dialN(t, h, url, kindRider, "r3", 1)
	b := dialN(t, h, url, kindRider, "r3", 2)
	waitFor(t, "subscription", func() bool { return subscribed(t, rdb, ch) })

	a.Close()
	waitFor(t, "first socket gone", func() bool { return h.count(kindRider, "r3") == 1 })
	time.Sleep(100 * time.Millisecond)
	if !subscribed(t, rdb, ch) {
		t.Fatal("unsubscribed while a socket was still open")
	}
	if err := h.pushToRider(ctx, "r3", message{Type: "ping", Payload: 1}); err != nil {
		t.Fatal(err)
	}
	if got := readText(t, b); !strings.Contains(got, "ping") {
		t.Errorf("remaining socket got %q", got)
	}

	b.Close()
	waitFor(t, "unsubscribe after the last close", func() bool { return !subscribed(t, rdb, ch) })
}
