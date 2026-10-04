// Command driver-app-sim stands in for the real Driver App: every 3-5
// seconds it reports a position for each of a set of drivers, each doing a
// small random walk from a starting point (US-3.1). Like the real app it goes
// through the Location Gateway with a token for that driver, and has no
// access to Kafka.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type simDriver struct {
	ID  string
	Lat float64
	Lng float64
}

// Matches the drivers matching-service seeds by default, so a
// simulator run without configuration lines up with a fresh seed.
var defaultDrivers = []simDriver{
	{"driver-1", 10.7769, 106.7009},
	{"driver-2", 10.7797, 106.6990},
	{"driver-3", 10.7720, 106.6980},
	{"driver-4", 10.7830, 106.7050},
	{"driver-5", 10.7700, 106.7100},
}

func loadDrivers() []simDriver {
	ids := os.Getenv("DRIVER_IDS")
	if ids == "" {
		return defaultDrivers
	}
	var drivers []simDriver
	for _, id := range strings.Split(ids, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		// Start near central Ho Chi Minh City if not one of the known seed drivers.
		lat, lng := 10.776, 106.700
		for _, d := range defaultDrivers {
			if d.ID == id {
				lat, lng = d.Lat, d.Lng
			}
		}
		drivers = append(drivers, simDriver{ID: id, Lat: lat, Lng: lng})
	}
	return drivers
}

func randomWalk(d *simDriver) {
	const stepDegrees = 0.0015 // roughly a short block per tick
	d.Lat += (rand.Float64()*2 - 1) * stepDegrees
	d.Lng += (rand.Float64()*2 - 1) * stepDegrees
}

// client reports positions the way the real Driver App will: over HTTP to the
// Location Gateway, with a token for that driver. It never touches Kafka.
type client struct {
	http       *http.Client
	authURL    string
	gatewayURL string

	mu     sync.Mutex
	tokens map[string]token
}

type token struct {
	value   string
	expires time.Time
}

func (c *client) tokenFor(ctx context.Context, driverID string, force bool) (string, error) {
	c.mu.Lock()
	t, ok := c.tokens[driverID]
	c.mu.Unlock()
	if ok && !force && time.Until(t.expires) > time.Minute {
		return t.value, nil
	}

	body, _ := json.Marshal(map[string]string{"role": "driver", "id": driverID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.authURL+"/token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth-service answered %d", res.StatusCode)
	}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return "", err
	}
	c.mu.Lock()
	c.tokens[driverID] = token{out.Token, out.ExpiresAt}
	c.mu.Unlock()
	return out.Token, nil
}

// report sends one position, getting a new token once if the gateway says the old one is no good.
func (c *client) report(ctx context.Context, d simDriver) error {
	body, _ := json.Marshal(map[string]any{"lat": d.Lat, "lng": d.Lng, "timestamp": time.Now().UTC().Format(time.RFC3339)})
	for attempt := 0; ; attempt++ {
		tok, err := c.tokenFor(ctx, d.ID, attempt > 0)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.gatewayURL+"/locations", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		res, err := c.http.Do(req)
		if err != nil {
			return err
		}
		res.Body.Close()
		switch {
		case res.StatusCode == http.StatusUnauthorized && attempt == 0:
			continue
		case res.StatusCode >= 300:
			return fmt.Errorf("gateway answered %d", res.StatusCode)
		}
		return nil
	}
}

func simulateDriver(ctx context.Context, wg *sync.WaitGroup, c *client, d simDriver) {
	defer wg.Done()

	for {
		randomWalk(&d)

		if err := c.report(ctx, d); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("driver=%s failed to report location: %v", d.ID, err)
		} else {
			log.Printf("driver=%s lat=%.5f lng=%.5f reported", d.ID, d.Lat, d.Lng)
		}

		// US-3.1: every 3-5 seconds.
		delay := 3*time.Second + time.Duration(rand.Intn(2001))*time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func main() {
	drivers := loadDrivers()
	c := &client{
		http:       &http.Client{Timeout: 10 * time.Second},
		authURL:    getEnv("AUTH_URL", "http://localhost:3005"),
		gatewayURL: getEnv("GATEWAY_URL", "http://localhost:3006"),
		tokens:     map[string]token{},
	}

	log.Printf("driver-app-sim reporting for %d driver(s) via %s: %v", len(drivers), c.gatewayURL, driverIDs(drivers))

	var wg sync.WaitGroup
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for _, d := range drivers {
		wg.Add(1)
		go simulateDriver(ctx, &wg, c, d)
	}
	wg.Wait()
}

func driverIDs(drivers []simDriver) []string {
	ids := make([]string, len(drivers))
	for i, d := range drivers {
		ids[i] = d.ID
	}
	return ids
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
