package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// shrink makes the socket limits small enough to hit in a test.
func shrink(t *testing.T, readLimit int64, pong, ping time.Duration) {
	t.Helper()
	limitsMu.Lock()
	old := limits
	limits.readLimit, limits.pongWait, limits.pingEvery = readLimit, pong, ping
	limitsMu.Unlock()
	t.Cleanup(func() { limitsMu.Lock(); limits = old; limitsMu.Unlock() })
}

// socketServer runs serveWS with no hub and reports when a socket is unregistered.
func socketServer(t *testing.T) (url string, closed chan struct{}) {
	t.Helper()
	closed = make(chan struct{}, 4)
	up := newUpgrader(nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveWS(up, w, r,
			func(ws *websocket.Conn) *conn { return &conn{ws: ws} },
			func(*conn) { closed <- struct{}{} })
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), closed
}

func expectClosed(t *testing.T, closed chan struct{}, why string) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal(why)
	}
}

func TestAFrameOverTheReadLimitDropsTheSocket(t *testing.T) {
	shrink(t, 16, time.Minute, time.Minute)
	url, closed := socketServer(t)
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	if err := ws.WriteMessage(websocket.TextMessage, []byte(strings.Repeat("x", 1<<20))); err != nil {
		t.Logf("write: %v", err) // the server may close before the whole frame is sent
	}
	expectClosed(t, closed, "a frame over the read limit did not close the socket")
}

func TestAClientThatNeverAnswersPingsIsDropped(t *testing.T) {
	shrink(t, 512, 200*time.Millisecond, 50*time.Millisecond)
	url, closed := socketServer(t)
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close() // never reads, so never answers a ping
	expectClosed(t, closed, "a silent client kept its socket past the pong deadline")
}

func TestAClientThatAnswersPingsKeepsItsSocket(t *testing.T) {
	shrink(t, 512, 200*time.Millisecond, 50*time.Millisecond)
	url, closed := socketServer(t)
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	go func() { // reading is what makes the client answer pings
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()

	select {
	case <-closed:
		t.Fatal("a client that answers pings was dropped")
	case <-time.After(800 * time.Millisecond): // four pong deadlines
	}
}

// Redis is not reachable here: register logs that it could not subscribe, and
// still tracks the socket, which is all this test is about.
func TestAReplacedSocketIsClosedNotLeftOpen(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer rdb.Close()
	h := newHub(ctx, rdb)
	up := newUpgrader(nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveWS(up, w, r,
			func(ws *websocket.Conn) *conn { return h.register(ctx, kindRider, "rider-1", ws) },
			func(c *conn) { h.unregister(ctx, kindRider, "rider-1", c) })
	}))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	first, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// Wait until the first is registered before the second replaces it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		h.mu.RLock()
		n := len(h.riderConns)
		h.mu.RUnlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	second, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	first.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err = first.ReadMessage()
	if ne, ok := err.(interface{ Timeout() bool }); err == nil || (ok && ne.Timeout()) {
		t.Fatalf("the replaced socket was left open (read gave %v)", err)
	}
}
