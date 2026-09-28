package main

import (
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// conn wraps a websocket connection with a write mutex: gorilla/websocket
// forbids concurrent writes from multiple goroutines to the same
// connection, and the hub can be pushing from the Kafka consumer
// goroutine while the connection's own reader is running.
type conn struct {
	ws *websocket.Conn
	mu sync.Mutex
}

func (c *conn) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.WriteJSON(v)
}

type message struct {
	Type    string `json:"type"`
	Payload any    `json:"payload"`
}

// hub is the Notification Service's in-memory state (design.md: "stateless,
// in-memory socket map"): open connections plus the routing info needed
// to turn a Kafka event into "which socket(s) does this go to".
type hub struct {
	mu sync.RWMutex

	riderConns  map[string]*conn
	driverConns map[string]*conn

	// orderRider learns the rider for an order from order.created, so
	// order.matched / order.no_driver_found (which don't carry riderId)
	// can still be routed to the right rider (US-5.1).
	orderRider map[string]string

	// driverRider tracks which rider a driver's *current* trip belongs
	// to, so driver.location.updated (keyed only by driverId) can be
	// routed to the rider watching that trip (task 4.3).
	driverRider map[string]string
}

func newHub() *hub {
	return &hub{
		riderConns:  make(map[string]*conn),
		driverConns: make(map[string]*conn),
		orderRider:  make(map[string]string),
		driverRider: make(map[string]string),
	}
}

func (h *hub) registerRider(riderID string, ws *websocket.Conn) *conn {
	c := &conn{ws: ws}
	h.mu.Lock()
	h.riderConns[riderID] = c
	h.mu.Unlock()
	return c
}

func (h *hub) registerDriver(driverID string, ws *websocket.Conn) *conn {
	c := &conn{ws: ws}
	h.mu.Lock()
	h.driverConns[driverID] = c
	h.mu.Unlock()
	return c
}

func (h *hub) unregisterRider(riderID string, c *conn) {
	h.mu.Lock()
	if h.riderConns[riderID] == c {
		delete(h.riderConns, riderID)
	}
	h.mu.Unlock()
}

func (h *hub) unregisterDriver(driverID string, c *conn) {
	h.mu.Lock()
	if h.driverConns[driverID] == c {
		delete(h.driverConns, driverID)
	}
	h.mu.Unlock()
}

func (h *hub) rememberOrderRider(orderID, riderID string) {
	h.mu.Lock()
	h.orderRider[orderID] = riderID
	h.mu.Unlock()
}

func (h *hub) riderForOrder(orderID string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	riderID, ok := h.orderRider[orderID]
	return riderID, ok
}

func (h *hub) rememberDriverRider(driverID, riderID string) {
	h.mu.Lock()
	h.driverRider[driverID] = riderID
	h.mu.Unlock()
}

func (h *hub) riderForDriver(driverID string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	riderID, ok := h.driverRider[driverID]
	return riderID, ok
}

func (h *hub) forgetDriverRider(driverID string) {
	h.mu.Lock()
	delete(h.driverRider, driverID)
	h.mu.Unlock()
}

// pushToRider sends msg to riderID's connection, if any is currently open.
func (h *hub) pushToRider(riderID string, msg message) {
	h.mu.RLock()
	c, ok := h.riderConns[riderID]
	h.mu.RUnlock()
	if !ok {
		return
	}
	if err := c.writeJSON(msg); err != nil {
		log.Printf("failed to push %s to rider=%s: %v", msg.Type, riderID, err)
	}
}

// pushToDriver sends msg to driverID's connection, if any is currently open.
func (h *hub) pushToDriver(driverID string, msg message) {
	h.mu.RLock()
	c, ok := h.driverConns[driverID]
	h.mu.RUnlock()
	if !ok {
		return
	}
	if err := c.writeJSON(msg); err != nil {
		log.Printf("failed to push %s to driver=%s: %v", msg.Type, driverID, err)
	}
}
