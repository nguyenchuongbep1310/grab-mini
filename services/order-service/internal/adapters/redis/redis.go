// Package redis holds the Redis adapters: the surge multiplier read and the
// single-use claims of quotes and promo tokens.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"math"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"grab-mini/order-service/internal/app"
	"grab-mini/pkg/surge"
)

// readTimeout keeps a struggling Redis from slowing every booking.
const readTimeout = 300 * time.Millisecond

// Multipliers reads what Pricing Service stores: surge:mult:{vehicle}:{cell}.
type Multipliers struct {
	rdb *goredis.Client
	m   app.Metrics
}

// NewMultipliers returns a MultiplierSource that fails open to 1.0, counting
// each failed read with m.SurgeFailOpen.
func NewMultipliers(rdb *goredis.Client, m app.Metrics) *Multipliers {
	return &Multipliers{rdb: rdb, m: m}
}

func (r *Multipliers) Multiplier(ctx context.Context, vehicle, cell string) float64 {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	v, err := r.rdb.Get(ctx, surge.MultKey(vehicle, cell)).Result()
	if err == goredis.Nil {
		return 1.0
	}
	if err != nil {
		r.m.SurgeFailOpen()
		log.Printf("surge multiplier read failed, pricing at 1.0: %v", err)
		return 1.0
	}
	mult, err := strconv.ParseFloat(v, 64)
	if err != nil || mult < 1 || math.IsNaN(mult) || math.IsInf(mult, 0) {
		return 1.0
	}
	return mult
}

// Claims stores the marks as quote:used:{sha256(id)}, SET NX with a TTL.
type Claims struct{ rdb *goredis.Client }

// NewClaims returns the ClaimStore over rdb.
func NewClaims(rdb *goredis.Client) *Claims { return &Claims{rdb: rdb} }

func claimKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "quote:used:" + hex.EncodeToString(sum[:])
}

func (c *Claims) Claim(ctx context.Context, id string, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	return c.rdb.SetNX(ctx, claimKey(id), "1", ttl).Result()
}

// Release takes the mark back. It ignores the cancellation of ctx: a booking
// that failed because its request was cancelled must still give its claims back.
func (c *Claims) Release(ctx context.Context, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readTimeout)
	defer cancel()
	if err := c.rdb.Del(ctx, claimKey(id)).Err(); err != nil {
		log.Printf("releasing quote after a failed booking: %v (it stays used until it expires)", err)
	}
}
