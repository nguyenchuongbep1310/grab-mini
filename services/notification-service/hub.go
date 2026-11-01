package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
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
		Help: "WebSockets (not people: one person may hold several) currently open on this instance, by kind (rider or driver).",
	}, []string{"kind"})

	socketsEvicted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_sockets_evicted_total",
		Help: "Oldest sockets closed because one person exceeded MAX_SOCKETS_PER_USER, by kind.",
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
// connection. Pushes from the hub go through a small per-socket queue drained
// by one writer goroutine, so a stuck socket only ever delays itself.
type conn struct {
	ws *websocket.Conn
	mu sync.Mutex

	out  chan []byte   // pending pushes; nil for a conn the hub does not own
	done chan struct{} // closed when the hub forgets the conn
	once sync.Once
}

// outQueue is how many pushes may wait for one socket before it counts as
// stuck and is dropped.
const outQueue = 32

func (c *conn) writeText(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A client that stops reading must not stall the hub's delivery loop.
	c.ws.SetWriteDeadline(time.Now().Add(currentLimits().writeWait))
	return c.ws.WriteMessage(websocket.TextMessage, b)
}

func (c *conn) stop() { c.once.Do(func() { close(c.done) }) }

// writeLoop writes queued pushes in order. A failed write (including the
// write deadline passing) closes the socket; its reader then unregisters it.
func (c *conn) writeLoop(kind, id string) {
	for {
		select {
		case <-c.done:
			return
		case b := <-c.out:
			if err := c.writeText(b); err != nil {
				pushes.WithLabelValues("delivered", "error").Inc()
				log.Printf("failed to push to %s=%s: %v", kind, id, err)
				c.ws.Close()
				return
			}
			pushes.WithLabelValues("delivered", "ok").Inc()
		}
	}
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

// defaultMaxSocketsPerUser caps simultaneous sockets for one rider or driver
// (MAX_SOCKETS_PER_USER). Several are normal (phone and web); the cap only
// stops a buggy client piling them up.
const defaultMaxSocketsPerUser = 5

type connKey struct{ kind, id string }

// hub holds this instance's open sockets and its Redis state.
type hub struct {
	rdb *redis.Client
	sub *redis.PubSub

	// subMu serialises "change the socket set + (un)subscribe" so the Redis
	// subscription always matches whether the set is empty, even when the
	// last socket closes while a new one connects. mu only guards the map, so
	// delivery never waits on a Redis round trip.
	subMu      sync.Mutex
	mu         sync.RWMutex
	conns      map[connKey][]*conn // oldest first
	maxSockets int
}

func newHub(ctx context.Context, rdb *redis.Client) *hub {
	return &hub{
		rdb: rdb,
		// A channel to hold the subscription open before any socket exists.
		sub:        rdb.Subscribe(ctx, "notify:hub"),
		conns:      make(map[connKey][]*conn),
		maxSockets: defaultMaxSocketsPerUser,
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

// deliver queues the payload on every socket the recipient has here. It never
// blocks on a socket: one that cannot keep up is closed.
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
	cs := append([]*conn(nil), h.conns[connKey{kind, id}]...)
	h.mu.RUnlock()
	for _, c := range cs {
		select {
		case c.out <- payload:
		default:
			pushes.WithLabelValues("delivered", "error").Inc()
			log.Printf("dropping stuck socket of %s=%s: %d pushes queued", kind, id, outQueue)
			c.ws.Close()
		}
	}
}

// register adds a socket for the person. The first one subscribes to their
// channel. Beyond maxSockets the OLDEST socket is closed.
func (h *hub) register(ctx context.Context, kind, id string, ws *websocket.Conn) *conn {
	c := &conn{ws: ws, out: make(chan []byte, outQueue), done: make(chan struct{})}
	key := connKey{kind, id}
	go c.writeLoop(kind, id)

	h.subMu.Lock()
	defer h.subMu.Unlock()
	h.mu.Lock()
	first := len(h.conns[key]) == 0
	cs := append(h.conns[key], c)
	socketsOpen.WithLabelValues(kind).Inc()
	var evicted []*conn
	limit := max(h.maxSockets, 1)
	for len(cs) > limit {
		evicted = append(evicted, cs[0])
		cs = cs[1:]
		socketsOpen.WithLabelValues(kind).Dec()
		socketsEvicted.WithLabelValues(kind).Inc()
	}
	h.conns[key] = cs
	h.mu.Unlock()
	for _, old := range evicted {
		log.Printf("evicting oldest socket of %s=%s: more than %d open", kind, id, limit)
		old.stop()
		old.ws.Close()
	}
	if first {
		if err := h.sub.Subscribe(ctx, channelFor(kind, id)); err != nil {
			log.Printf("failed to subscribe to %s=%s: %v", kind, id, err)
		}
	}
	return c
}

// unregister forgets c only (an evicted conn is already gone). The last
// socket of a person unsubscribes from their channel.
func (h *hub) unregister(ctx context.Context, kind, id string, c *conn) {
	c.stop()
	key := connKey{kind, id}
	h.subMu.Lock()
	defer h.subMu.Unlock()
	h.mu.Lock()
	cs := h.conns[key]
	i := slices.Index(cs, c)
	if i < 0 {
		h.mu.Unlock()
		return
	}
	cs = slices.Delete(slices.Clone(cs), i, i+1)
	socketsOpen.WithLabelValues(kind).Dec()
	last := len(cs) == 0
	if last {
		delete(h.conns, key)
	} else {
		h.conns[key] = cs
	}
	h.mu.Unlock()
	if !last {
		return
	}
	if err := h.sub.Unsubscribe(ctx, channelFor(kind, id)); err != nil {
		log.Printf("failed to unsubscribe from %s=%s: %v", kind, id, err)
	}
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
