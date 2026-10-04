package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func ok(name string) Check { return Check{name, func(context.Context) error { return nil }} }

func down(name string) Check {
	return Check{name, func(context.Context) error { return errors.New(name + " is down") }}
}

func get(h http.Handler) (int, map[string]any) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestLiveIgnoresDependencies(t *testing.T) {
	if code, _ := get(Live()); code != http.StatusOK {
		t.Errorf("livez = %d, want 200", code)
	}
}

func TestReadyWhenEveryDependencyAnswers(t *testing.T) {
	code, body := get(Ready(context.Background(), ok("postgres"), ok("kafka")))
	if code != http.StatusOK || body["status"] != "ok" {
		t.Errorf("readyz = %d %v, want 200 ok", code, body)
	}
}

func TestNotReadyNamesTheDependencyThatIsDown(t *testing.T) {
	code, body := get(Ready(context.Background(), ok("postgres"), down("kafka")))
	checks, _ := body["checks"].(map[string]any)
	if code != http.StatusServiceUnavailable || checks["kafka"] != "kafka is down" || checks["postgres"] != "ok" {
		t.Errorf("readyz = %d %v, want 503 naming kafka", code, body)
	}
}

func TestNotReadyOnceShutdownBegins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := Ready(ctx, ok("postgres"))
	if code, _ := get(h); code != http.StatusOK {
		t.Fatalf("before shutdown readyz = %d", code)
	}
	cancel()
	if code, body := get(h); code != http.StatusServiceUnavailable || body["status"] != "shutting down" {
		t.Errorf("after shutdown began readyz = %d %v, want 503", code, body)
	}
}

func TestAHungDependencyFailsTheProbeInsteadOfHangingIt(t *testing.T) {
	hung := Check{"redis", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	started := time.Now()
	code, _ := get(Ready(context.Background(), hung))
	if code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503", code)
	}
	if took := time.Since(started); took > CheckTimeout+time.Second {
		t.Errorf("probe took %s, want it cut off near %s", took, CheckTimeout)
	}
}

func TestChecksRunConcurrently(t *testing.T) {
	slow := func(name string) Check {
		return Check{name, func(context.Context) error { time.Sleep(300 * time.Millisecond); return nil }}
	}
	started := time.Now()
	get(Ready(context.Background(), slow("a"), slow("b"), slow("c")))
	if took := time.Since(started); took > 700*time.Millisecond {
		t.Errorf("three 300ms checks took %s; they should overlap", took)
	}
}

func TestKafkaCheckAgainstAnUnreachableBrokerFailsQuickly(t *testing.T) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), CheckTimeout)
	defer cancel()
	if err := Kafka([]string{"127.0.0.1:1"}).Fn(ctx); err == nil {
		t.Error("an unreachable broker reported ready")
	}
	if took := time.Since(started); took > CheckTimeout+time.Second {
		t.Errorf("took %s", took)
	}
}

// Real dependencies: set TEST_KAFKA_BROKERS, TEST_REDIS_ADDR, TEST_DATABASE_URL.
func TestKafkaCheckAgainstARealBroker(t *testing.T) {
	b := os.Getenv("TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	if err := Kafka([]string{b}).Fn(context.Background()); err != nil {
		t.Errorf("a running broker reported not ready: %v", err)
	}
}

func TestRedisAndPostgresChecksAgainstRealServers(t *testing.T) {
	addr, url := os.Getenv("TEST_REDIS_ADDR"), os.Getenv("TEST_DATABASE_URL")
	if addr == "" || url == "" {
		t.Skip("TEST_REDIS_ADDR and TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if err := Redis(rdb).Fn(ctx); err != nil {
		t.Errorf("redis: %v", err)
	}
	if err := Postgres(pool).Fn(ctx); err != nil {
		t.Errorf("postgres: %v", err)
	}

	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	if err := Redis(dead).Fn(ctx); err == nil {
		t.Error("a dead redis reported ready")
	}
}
