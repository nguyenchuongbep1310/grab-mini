package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

var (
	socketsOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notification_sockets_open",
		Help: "WebSockets currently open on this instance, by kind (rider or driver).",
	}, []string{"kind"})

	// delivered counts messages this instance wrote to a socket it holds;
	// published counts messages it sent to Redis for some instance to deliver.
	// Their difference across the fleet is what never found a listener (a rider
	// with no open socket), which is normal and not an error.
	pushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_pushes_total",
		Help: "Notification messages, by stage (published to Redis, delivered to a socket) and outcome.",
	}, []string{"stage", "result"})
)

// conn wraps a websocket connection with a write mutex: gorilla/websocket
// forbids concurrent writes from multiple goroutines to the same
// connection, and the hub can be delivering from its Redis subscription
// goroutine while the connection's own reader is running.
type conn struct {
	ws *websocket.Conn
	mu sync.Mutex
}

func (c *conn) writeText(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A client that stops reading must not stall the hub's delivery loop.
	c.ws.SetWriteDeadline(time.Now().Add(currentLimits().writeWait))
	return c.ws.WriteMessage(websocket.TextMessage, b)
}

type message struct {
	Type    string `json:"type"`
	Payload any    `json:"payload"`
}

// How the service scales out. Kafka splits the topics' partitions between
// instances, so the instance that consumes an event is usually not the one
// holding the rider's or driver's socket. So an instance never writes to a
// socket on its own say-so from a Kafka event: it publishes the message to a
// Redis channel named for the recipient, and the instance that holds that
// recipient's socket (it subscribed when the socket connected) writes it.
// Delivery is best-effort, like the socket itself: a message published while
// nobody is subscribed is dropped, and the apps recover by asking the REST
// APIs after a reconnect.

const (
	kindRider  = "rider"
	kindDriver = "driver"

	// Routing state outlives any trip; it only has to outlive the gap
	// between two related events.
	routeTTL = 6 * time.Hour
)

func channelFor(kind, id string) string { return "notify:" + kind + ":" + id }

// hub holds this instance's open sockets and its Redis state.
type hub struct {
	rdb *redis.Client
	sub *redis.PubSub

	mu          sync.RWMutex
	riderConns  map[string]*conn
	driverConns map[string]*conn
}

func newHub(ctx context.Context, rdb *redis.Client) *hub {
	return &hub{
		rdb: rdb,
		// A channel to hold the subscription open before any socket exists.
		sub:         rdb.Subscribe(ctx, "notify:hub"),
		riderConns:  make(map[string]*conn),
		driverConns: make(map[string]*conn),
	}
}

// run delivers what arrives on the subscribed channels to the local sockets,
// until ctx is cancelled.
func (h *hub) run(ctx context.Context) {
	defer h.sub.Close()
	ch := h.sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			h.deliver(m.Channel, []byte(m.Payload))
		}
	}
}

func (h *hub) deliver(channel string, payload []byte) {
	rest, ok := strings.CutPrefix(channel, "notify:")
	if !ok {
		return
	}
	kind, id, ok := strings.Cut(rest, ":")
	if !ok {
		return
	}

	h.mu.RLock()
	var c *conn
	switch kind {
	case kindRider:
		c = h.riderConns[id]
	case kindDriver:
		c = h.driverConns[id]
	}
	h.mu.RUnlock()
	if c == nil {
		return
	}
	if err := c.writeText(payload); err != nil {
		pushes.WithLabelValues("delivered", "error").Inc()
		log.Printf("failed to push to %s=%s: %v", kind, id, err)
		return
	}
	pushes.WithLabelValues("delivered", "ok").Inc()
}

func (h *hub) register(ctx context.Context, kind, id string, ws *websocket.Conn) *conn {
	c := &conn{ws: ws}
	h.mu.Lock()
	old, replaced := h.connsFor(kind)[id]
	if !replaced {
		socketsOpen.WithLabelValues(kind).Inc()
	}
	h.connsFor(kind)[id] = c
	h.mu.Unlock()
	// One socket per person: a reconnect replaces the old one, which would
	// otherwise stay open (and read) until its client chose to go. Without
	// this a client could pile up sockets by reconnecting.
	if replaced {
		old.ws.Close()
	}
	if err := h.sub.Subscribe(ctx, channelFor(kind, id)); err != nil {
		log.Printf("failed to subscribe to %s=%s: %v", kind, id, err)
	}
	return c
}

// unregister forgets c, unless the same id has already reconnected with a
// newer connection (then that one keeps the subscription).
func (h *hub) unregister(ctx context.Context, kind, id string, c *conn) {
	h.mu.Lock()
	current := h.connsFor(kind)[id] == c
	if current {
		delete(h.connsFor(kind), id)
		socketsOpen.WithLabelValues(kind).Dec()
	}
	h.mu.Unlock()
	if !current {
		return
	}
	if err := h.sub.Unsubscribe(ctx, channelFor(kind, id)); err != nil {
		log.Printf("failed to unsubscribe from %s=%s: %v", kind, id, err)
	}
}

func (h *hub) connsFor(kind string) map[string]*conn {
	if kind == kindRider {
		return h.riderConns
	}
	return h.driverConns
}

// push sends msg to whichever instance holds the recipient's socket.
func (h *hub) push(ctx context.Context, kind, id string, msg message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = h.rdb.Publish(ctx, channelFor(kind, id), payload).Err()
	if err != nil {
		pushes.WithLabelValues("published", "error").Inc()
		return err
	}
	pushes.WithLabelValues("published", "ok").Inc()
	return nil
}

func (h *hub) pushToRider(ctx context.Context, riderID string, msg message) error {
	return h.push(ctx, kindRider, riderID, msg)
}

func (h *hub) pushToDriver(ctx context.Context, driverID string, msg message) error {
	return h.push(ctx, kindDriver, driverID, msg)
}

// Routing state, in Redis so any instance can answer. Most events carry the
// rider id themselves; this covers events written before they did, and
// driver.location.updated, which only knows the driver.

func orderRiderKey(orderID string) string   { return "notify:order-rider:" + orderID }
func driverRiderKey(driverID string) string { return "notify:driver-rider:" + driverID }

func (h *hub) rememberOrderRider(ctx context.Context, orderID, riderID string) error {
	return h.rdb.Set(ctx, orderRiderKey(orderID), riderID, routeTTL).Err()
}

func (h *hub) riderForOrder(ctx context.Context, orderID string) (string, error) {
	return getOrEmpty(ctx, h.rdb, orderRiderKey(orderID))
}

func (h *hub) rememberDriverRider(ctx context.Context, driverID, riderID string) error {
	return h.rdb.Set(ctx, driverRiderKey(driverID), riderID, routeTTL).Err()
}

func (h *hub) riderForDriver(ctx context.Context, driverID string) (string, error) {
	return getOrEmpty(ctx, h.rdb, driverRiderKey(driverID))
}

// The driver an order was matched to, so order.cancelled (which names no
// driver) can also reach them.
func orderDriverKey(orderID string) string { return "notify:order-driver:" + orderID }

func (h *hub) rememberOrderDriver(ctx context.Context, orderID, driverID string) error {
	return h.rdb.Set(ctx, orderDriverKey(orderID), driverID, routeTTL).Err()
}

func (h *hub) driverForOrder(ctx context.Context, orderID string) (string, error) {
	return getOrEmpty(ctx, h.rdb, orderDriverKey(orderID))
}

func (h *hub) forgetDriverRider(ctx context.Context, driverID string) error {
	return h.rdb.Del(ctx, driverRiderKey(driverID)).Err()
}

func getOrEmpty(ctx context.Context, rdb *redis.Client, key string) (string, error) {
	v, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}
