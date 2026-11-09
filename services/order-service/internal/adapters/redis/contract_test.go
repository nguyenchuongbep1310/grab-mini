package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"grab-mini/order-service/internal/app"
	"grab-mini/pkg/surge"
)

var (
	_ app.MultiplierSource = (*Multipliers)(nil)
	_ app.ClaimStore       = (*Claims)(nil)
)

func testRedis(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	if strings.HasSuffix(addr, ":6379") {
		t.Fatalf("refusing to run against %s: the default Redis port may be a real instance", addr)
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

type countingMetrics struct {
	app.Metrics
	failOpen int
}

func (c *countingMetrics) SurgeFailOpen() { c.failOpen++ }

// The claim key is what the flat code wrote: other replicas still running the
// old build must see the same marks during a rollout.
func TestTheClaimKeyFormatIsTheOldOne(t *testing.T) {
	sum := sha256.Sum256([]byte("q-1"))
	want := "quote:used:" + hex.EncodeToString(sum[:])
	if got := claimKey("q-1"); got != want {
		t.Errorf("claimKey = %q, want %q", got, want)
	}
	sum = sha256.Sum256([]byte("promo:tok"))
	if got := claimKey("promo:tok"); got != "quote:used:"+hex.EncodeToString(sum[:]) {
		t.Errorf("a promo claim hashes the whole id, got %q", got)
	}
}

func TestQuoteMarksUseSetNXWithATTL(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	c := NewClaims(rdb)

	first, err := c.Claim(ctx, "q-1", time.Minute)
	if err != nil || !first {
		t.Fatalf("first claim = %v, %v; want true", first, err)
	}
	again, err := c.Claim(ctx, "q-1", time.Minute)
	if err != nil || again {
		t.Errorf("second claim = %v, %v; want false (SET NX)", again, err)
	}
	ttl, err := rdb.TTL(ctx, claimKey("q-1")).Result()
	if err != nil || ttl <= 0 || ttl > time.Minute {
		t.Errorf("ttl = %v, %v; want within (0, 1m]", ttl, err)
	}
	if other, _ := c.Claim(ctx, "promo:q-1", time.Minute); !other {
		t.Error("a promo claim must not collide with the quote of the same text")
	}
}

func TestReleaseGivesTheClaimBackEvenWhenTheRequestIsCancelled(t *testing.T) {
	rdb := testRedis(t)
	c := NewClaims(rdb)
	if first, err := c.Claim(context.Background(), "q-2", time.Minute); err != nil || !first {
		t.Fatalf("claim = %v, %v", first, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Release(ctx, "q-2")
	if n, err := rdb.Exists(context.Background(), claimKey("q-2")).Result(); err != nil || n != 0 {
		t.Errorf("claim key exists = %d, %v after Release on a cancelled ctx; want it gone", n, err)
	}
	if first, _ := c.Claim(context.Background(), "q-2", time.Minute); !first {
		t.Error("the quote can book again after its release")
	}
}

func TestRedisMultipliersReadsAndFailsOpen(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	m := &countingMetrics{}
	mult := NewMultipliers(rdb, m)
	key := surge.MultKey("CAR", "c1")

	if got := mult.Multiplier(ctx, "CAR", "c1"); got != 1.0 || m.failOpen != 0 {
		t.Errorf("missing key = %v (fail-open %d), want 1.0 and no fail-open", got, m.failOpen)
	}
	rdb.Set(ctx, key, "1.8", 0)
	if got := mult.Multiplier(ctx, "CAR", "c1"); got != 1.8 {
		t.Errorf("stored 1.8 = %v", got)
	}
	for _, bad := range []string{"NaN", "+Inf", "0.5", "abc", ""} {
		rdb.Set(ctx, key, bad, 0)
		if got := mult.Multiplier(ctx, "CAR", "c1"); got != 1.0 || math.IsNaN(got) {
			t.Errorf("stored %q = %v, want 1.0", bad, got)
		}
	}
	if m.failOpen != 0 {
		t.Errorf("a bad value is not a Redis failure: fail-open = %d", m.failOpen)
	}

	down := goredis.NewClient(&goredis.Options{Addr: "localhost:1"})
	defer down.Close()
	if got := NewMultipliers(down, m).Multiplier(ctx, "CAR", "c1"); got != 1.0 || m.failOpen != 1 {
		t.Errorf("Redis down = %v, fail-open %d; want 1.0 and one SurgeFailOpen", got, m.failOpen)
	}
	_ = errors.New
}
