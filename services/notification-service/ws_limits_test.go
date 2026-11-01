package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
