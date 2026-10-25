package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/tracing"
)

// Needs a real Redis (it checks Pub/Sub across two "instances"): set
// TEST_REDIS_ADDR to a throwaway instance; the tests FLUSH it.

// jsonDecoder stands in for the Avro codec: the test messages are JSON, and
// the event structs carry json tags for the WebSocket payloads anyway.
type jsonDecoder struct{}

func (jsonDecoder) Decode(_ context.Context, _ string, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

const testSecret = "0123456789abcdef0123456789abcdef"

type instance struct {
	h      *hub
	server *httptest.Server
	issuer *auth.Issuer
}

func newInstance(t *testing.T, ctx context.Context, addr string) *instance {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	h := newHub(ctx, rdb)
	go h.run(ctx)
	verifier, _ := auth.NewVerifier(testSecret)
	issuer, _ := auth.NewIssuer(testSecret, time.Hour)
	// Wrapped as main wraps it: the WebSocket upgrade must survive both middlewares.
	srv := httptest.NewServer(tracing.HTTP("notification-service", metrics.HTTP(newMux(ctx, h, verifier, []string{"http://app.example"}))))
	t.Cleanup(func() { srv.Close(); rdb.Close() })
	return &instance{h, srv, issuer}
}

func setup(t *testing.T) (context.Context, string) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return ctx, addr
}

func (in *instance) dial(kind, id string, subprotocols []string, header http.Header) (*websocket.Conn, *http.Response, error) {
	url := "ws" + strings.TrimPrefix(in.server.URL, "http") + "/ws/" + kind + "/" + id
	d := websocket.Dialer{Subprotocols: subprotocols}
	return d.Dial(url, header)
}

func (in *instance) connect(t *testing.T, kind, id string) *websocket.Conn {
	t.Helper()
	token, _, err := in.issuer.Issue(auth.Role(kind), id)
	if err != nil {
		t.Fatal(err)
	}
	ws, resp, err := in.dial(kind, id, []string{auth.WebSocketSubprotocol, token}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Sec-WebSocket-Protocol") != auth.WebSocketSubprotocol {
		t.Fatalf("the server answered with subprotocol %q; a browser would refuse the connection", resp.Header.Get("Sec-WebSocket-Protocol"))
	}
	t.Cleanup(func() { ws.Close() })

	// The server subscribes just after the handshake; a message published
	// before that is (by design) dropped, so wait for the subscription.
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, _ := in.h.rdb.PubSubNumSub(context.Background(), channelFor(kind, id)).Result()
		if n[channelFor(kind, id)] > 0 {
			return ws
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s never subscribed", kind, id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func receive(t *testing.T, ws *websocket.Conn) message {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	var raw struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := ws.ReadJSON(&raw); err != nil {
		t.Fatalf("expected a message: %v", err)
	}
	var payload map[string]any
	json.Unmarshal(raw.Payload, &payload)
	return message{Type: raw.Type, Payload: payload}
}

func expectNothing(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, data, err := ws.ReadMessage(); err == nil {
		t.Fatalf("unexpected message: %s", data)
	}
}

func kafkaMsg(t *testing.T, topic string, v any) kafka.Message {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: topic, Value: b}
}

func TestAnEventHandledByOneInstanceReachesASocketOnAnother(t *testing.T) {
	ctx, addr := setup(t)
	a, b := newInstance(t, ctx, addr), newInstance(t, ctx, addr)
	riderSocket := b.connect(t, kindRider, "rider-1") // the rider is on B...
	driverSocket := a.connect(t, kindDriver, "driver-1")

	// ...but A consumes the event.
	evt := events.OrderMatched{OrderID: "o1", DriverID: "driver-1", RiderID: "rider-1", VehicleType: events.VehicleCar}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderMatched, evt)); err != nil {
		t.Fatal(err)
	}

	if m := receive(t, riderSocket); m.Type != events.TopicOrderMatched || m.Payload.(map[string]any)["driverId"] != "driver-1" {
		t.Errorf("rider got %+v", m)
	}
	if m := receive(t, driverSocket); m.Type != events.TopicOrderMatched {
		t.Errorf("driver got %+v", m)
	}
}

func TestAnEventForSomeoneElseIsNotDelivered(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	other := a.connect(t, kindRider, "rider-2")

	evt := events.OrderNoDriverFound{OrderID: "o1", RiderID: "rider-1"}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderNoDriverFound, evt)); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, other)
}

// Messages from before the events carried riderId are still routed, using
// the order.created that any instance may have seen.
func TestAnOlderEventWithoutRiderIdIsRoutedByTheRememberedOrder(t *testing.T) {
	ctx, addr := setup(t)
	a, b := newInstance(t, ctx, addr), newInstance(t, ctx, addr)
	riderSocket := b.connect(t, kindRider, "rider-1")

	created := events.OrderCreated{OrderID: "o1", RiderID: "rider-1"}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderCreated, created)); err != nil {
		t.Fatal(err)
	}
	// order.matched is consumed by B this time, which never saw order.created.
	old := events.OrderMatched{OrderID: "o1", DriverID: "driver-1"} // no RiderID
	if err := handleMessage(ctx, jsonDecoder{}, b.h, kafkaMsg(t, events.TopicOrderMatched, old)); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, riderSocket); m.Type != events.TopicOrderMatched {
		t.Errorf("rider got %+v", m)
	}
}

func TestLocationUpdatesFollowTheTripAcrossInstances(t *testing.T) {
	ctx, addr := setup(t)
	a, b := newInstance(t, ctx, addr), newInstance(t, ctx, addr)
	riderSocket := b.connect(t, kindRider, "rider-1")

	handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderMatched,
		events.OrderMatched{OrderID: "o1", DriverID: "driver-1", RiderID: "rider-1"}))
	receive(t, riderSocket) // the match itself

	loc := kafkaMsg(t, events.TopicDriverLocationUpdate, events.DriverLocationUpdated{DriverID: "driver-1", Lat: 10.7, Lng: 106.7})
	if err := handleMessage(ctx, jsonDecoder{}, a.h, loc); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, riderSocket); m.Type != events.TopicDriverLocationUpdate {
		t.Fatalf("rider got %+v, want the driver's location", m)
	}

	// Once the trip is over the rider stops following the driver.
	done := events.TripStatusChanged{TripID: "t1", OrderID: "o1", DriverID: "driver-1", RiderID: "rider-1", Status: events.TripStatusCompleted}
	handleMessage(ctx, jsonDecoder{}, b.h, kafkaMsg(t, events.TopicTripStatusChanged, done))
	if m := receive(t, riderSocket); m.Type != events.TopicTripStatusChanged {
		t.Fatalf("rider got %+v, want the completed status", m)
	}
	handleMessage(ctx, jsonDecoder{}, a.h, loc)
	expectNothing(t, riderSocket)
}

func TestADisconnectedSocketIsUnsubscribed(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	ws := a.connect(t, kindRider, "rider-1")
	ws.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		n, _ := a.h.rdb.PubSubNumSub(ctx, channelFor(kindRider, "rider-1")).Result()
		if n[channelFor(kindRider, "rider-1")] == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("still subscribed after the socket closed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestAReconnectingRiderKeepsTheirSubscription(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	first := a.connect(t, kindRider, "rider-1")
	second := a.connect(t, kindRider, "rider-1") // reconnects before the first is noticed gone
	first.Close()
	time.Sleep(300 * time.Millisecond) // let the old connection's cleanup run

	evt := events.OrderNoDriverFound{OrderID: "o1", RiderID: "rider-1"}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderNoDriverFound, evt)); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, second); m.Type != events.TopicOrderNoDriverFound {
		t.Errorf("rider got %+v after reconnecting", m)
	}
}

// Who may open a socket.
func TestOnlyTheRightPersonCanOpenASocket(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	riderTok, _, _ := a.issuer.Issue(auth.Rider, "rider-1")
	driverTok, _, _ := a.issuer.Issue(auth.Driver, "driver-1")
	other, _ := auth.NewIssuer("another-secret-another-secret-123", time.Hour)
	forged, _, _ := other.Issue(auth.Rider, "rider-1")
	expired, _ := auth.NewIssuer(testSecret, -time.Hour)
	stale, _, _ := expired.Issue(auth.Rider, "rider-1")

	cases := []struct {
		name         string
		kind, id     string
		subprotocols []string
		want         int
	}{
		{"no token", kindRider, "rider-1", nil, 401},
		{"garbage token", kindRider, "rider-1", []string{"bearer", "junk"}, 401},
		{"signed with another secret", kindRider, "rider-1", []string{"bearer", forged}, 401},
		{"expired", kindRider, "rider-1", []string{"bearer", stale}, 401},
		{"someone else's id", kindRider, "rider-2", []string{"bearer", riderTok}, 403},
		{"a driver token on a rider socket", kindRider, "rider-1", []string{"bearer", driverTok}, 403},
		{"a rider token on a driver socket", kindDriver, "rider-1", []string{"bearer", riderTok}, 403},
	}
	for _, c := range cases {
		ws, resp, err := a.dial(c.kind, c.id, c.subprotocols, nil)
		if err == nil {
			ws.Close()
			t.Errorf("%s: the socket opened", c.name)
			continue
		}
		if resp == nil || resp.StatusCode != c.want {
			t.Errorf("%s: %v, want status %d", c.name, resp, c.want)
		}
	}
}

func TestASocketFromAnotherSitesPageIsRefused(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	tok, _, _ := a.issuer.Issue(auth.Rider, "rider-1")
	protocols := []string{"bearer", tok}

	if ws, _, err := a.dial(kindRider, "rider-1", protocols, http.Header{"Origin": {"http://evil.example"}}); err == nil {
		ws.Close()
		t.Error("a page on another origin opened a socket with a valid token")
	}
	if ws, _, err := a.dial(kindRider, "rider-1", protocols, http.Header{"Origin": {"http://app.example"}}); err != nil {
		t.Errorf("the allowed origin was refused: %v", err)
	} else {
		ws.Close()
	}
	// A non-browser client sends no Origin; the token is what protects it.
	if ws, _, err := a.dial(kindRider, "rider-1", protocols, nil); err != nil {
		t.Errorf("a client without an Origin was refused: %v", err)
	} else {
		ws.Close()
	}
}

func TestTheOpenSocketGaugeFollowsConnectsAndDisconnects(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	open := func() float64 { return testutil.ToFloat64(socketsOpen.WithLabelValues(kindRider)) }
	base := open()

	first := a.connect(t, kindRider, "gauge-rider")
	second := a.connect(t, kindRider, "gauge-rider") // the same rider reconnecting is still one socket
	if open()-base != 1 {
		t.Errorf("gauge moved by %v for one rider connecting twice, want 1", open()-base)
	}
	first.Close()
	second.Close()

	deadline := time.Now().Add(5 * time.Second)
	for open() != base {
		if time.Now().After(deadline) {
			t.Fatalf("gauge stuck at %v, want %v after both sockets closed", open(), base)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReconnectingClosesTheOlderSocket(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	first := a.connect(t, kindRider, "rider-1")
	second := a.connect(t, kindRider, "rider-1")
	defer second.Close()

	first.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := first.ReadMessage(); err == nil {
		t.Fatal("the replaced socket delivered a message instead of being closed")
	} else if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
		t.Fatal("the replaced socket was left open")
	}
}
