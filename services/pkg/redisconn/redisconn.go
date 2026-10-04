// Package redisconn builds the Redis client every service uses, so that
// whether Redis is a single node or a Sentinel-managed set is one deployment
// setting rather than a code change in each service.
//
//	REDIS_ADDR=host:6379                      one node (the default)
//	REDIS_SENTINEL_ADDRS=s1:26379,s2:26379    ask these Sentinels for the master
//	REDIS_MASTER_NAME=mymaster                the master's name in Sentinel (default "mymaster")
//	REDIS_PASSWORD / REDIS_PASSWORD_FILE      password for Redis itself (optional)
//	REDIS_SENTINEL_PASSWORD(_FILE)            password for the Sentinels (optional)
//
// With Sentinel the client follows a failover by itself: it re-asks the
// Sentinels for the new master when the old one stops answering, so a
// failover costs a few seconds of errors (retried by the consumers) instead
// of a restart of every service. Everything still talks to ONE master:
// Sentinel adds availability, not capacity (see docs/performance.md for
// capacity).
package redisconn

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

// New returns a client configured from the environment, falling back to addr
// when no Sentinels are configured.
func New(addr string) (*redis.Client, error) {
	password, err := optional("REDIS_PASSWORD")
	if err != nil {
		return nil, err
	}
	sentinels := splitList(os.Getenv("REDIS_SENTINEL_ADDRS"))
	if len(sentinels) == 0 {
		return redis.NewClient(&redis.Options{Addr: addr, Password: password}), nil
	}
	sentinelPassword, err := optional("REDIS_SENTINEL_PASSWORD")
	if err != nil {
		return nil, err
	}
	master := os.Getenv("REDIS_MASTER_NAME")
	if master == "" {
		master = "mymaster"
	}
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       master,
		SentinelAddrs:    sentinels,
		Password:         password,
		SentinelPassword: sentinelPassword,
	}), nil
}

// MustNew is New for a main() that can't continue without Redis config.
func MustNew(addr string) *redis.Client {
	c, err := New(addr)
	if err != nil {
		log.Fatalf("redis config: %v", err)
	}
	return c
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// optional reads NAME_FILE or NAME; both unset means no password.
func optional(name string) (string, error) {
	if path := os.Getenv(name + "_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: %w", name, err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	return os.Getenv(name), nil
}
