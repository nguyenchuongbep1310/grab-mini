package redisconn

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewIsASingleNodeClientUnlessSentinelsAreConfigured(t *testing.T) {
	t.Setenv("REDIS_SENTINEL_ADDRS", "")
	c, err := New("example:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.Options().Addr; got != "example:6379" {
		t.Errorf("Addr = %q, want example:6379", got)
	}
}

func TestSentinelConfigurationAndPasswordFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(path, []byte("s3cret\n"), 0o600)
	t.Setenv("REDIS_SENTINEL_ADDRS", " s1:26379, s2:26379 ,")
	t.Setenv("REDIS_MASTER_NAME", "")
	t.Setenv("REDIS_PASSWORD_FILE", path)
	c, err := New("ignored:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	o := c.Options()
	if o.Password != "s3cret" {
		t.Errorf("password = %q, want the file's content without its newline", o.Password)
	}
	if o.Addr == "ignored:6379" {
		t.Errorf("client dials %q: the explicit address must not be used with Sentinels", o.Addr)
	}
}

func TestAMissingPasswordFileIsAnError(t *testing.T) {
	t.Setenv("REDIS_PASSWORD_FILE", filepath.Join(t.TempDir(), "nope"))
	if _, err := New("x:6379"); err == nil {
		t.Fatal("unreadable password file accepted")
	}
}

// Needs a master with a replica and Sentinel monitoring it as "mymaster":
// set TEST_REDIS_SENTINEL_ADDRS (and see docs/performance.md for how this was
// run; Sentinel refuses a second failover for ~20 s). It asks Sentinel for a failover and requires writes to work again,
// through the same client, without restarting anything.
func TestTheClientFollowsAFailover(t *testing.T) {
	addrs := os.Getenv("TEST_REDIS_SENTINEL_ADDRS")
	if addrs == "" {
		t.Skip("TEST_REDIS_SENTINEL_ADDRS not set")
	}
	t.Setenv("REDIS_SENTINEL_ADDRS", addrs)
	t.Setenv("REDIS_MASTER_NAME", "mymaster")
	ctx := context.Background()
	c, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Set(ctx, "failover-test", "before", 0).Err(); err != nil {
		t.Fatalf("write before the failover: %v", err)
	}

	sentinel := redis.NewClient(&redis.Options{Addr: strings.Split(addrs, ",")[0]})
	defer sentinel.Close()
	masterAddr := func() string {
		a, err := sentinel.Do(ctx, "SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster").StringSlice()
		if err != nil || len(a) != 2 {
			return ""
		}
		return a[0] + ":" + a[1]
	}
	oldMaster := masterAddr()
	if err := sentinel.Do(ctx, "SENTINEL", "FAILOVER", "mymaster").Err(); err != nil {
		t.Fatalf("asking Sentinel for a failover: %v", err)
	}

	// The failover has really happened once Sentinel names another master.
	deadline := time.Now().Add(30 * time.Second)
	for masterAddr() == oldMaster || masterAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatalf("Sentinel still reports %s as master 30s after the failover request", oldMaster)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("master moved %s -> %s", oldMaster, masterAddr())

	// The same client, never restarted, must write to the new master.
	start := time.Now()
	deadline = start.Add(30 * time.Second)
	for {
		err := c.Set(ctx, "failover-test", "after", 0).Err()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("writes still failing 30s after the master moved: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("writes worked %v after Sentinel announced the new master", time.Since(start).Round(100*time.Millisecond))

	// Sentinel demotes the old master a moment after promoting the new one;
	// until then it could accept writes that would be lost.
	old := redis.NewClient(&redis.Options{Addr: oldMaster})
	defer old.Close()
	deadline = time.Now().Add(15 * time.Second)
	for old.Set(ctx, "failover-test", "split-brain", 0).Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the old master still accepts writes 15s after the failover")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
