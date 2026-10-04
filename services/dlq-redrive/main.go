// Command dlq-redrive sends the messages on a dead-letter topic back to the
// topic they came from, once whatever made them fail has been fixed.
//
//	dlq-redrive -topic order.created -dry-run          # see what is waiting
//	dlq-redrive -topic order.created                    # redrive all of it
//	dlq-redrive -topic order.created -error "no such column" -max 100
//
// It needs an operator's credentials, not a service's: the service principals
// can write their own dead-letter topics but not read them (scripts/kafka-acls.sh).
// Configure the connection with the same KAFKA_* variables as the services.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		topic  = flag.String("topic", "", "original topic whose dead letters to redrive, e.g. order.created (required)")
		group  = flag.String("group", "", "consumer group that remembers progress (default: dlq-redrive-<topic>)")
		dryRun = flag.Bool("dry-run", false, "list what would be redriven; change nothing")
		max    = flag.Int("max", 0, "redrive at most this many (0: all)")
		errSub = flag.String("error", "", "only dead letters whose error contains this text")
		idle   = flag.Duration("idle", 3*time.Second, "stop after this long with no new dead letter")
	)
	flag.Parse()

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	g := *group
	if g == "" {
		// A filtered redrive has to read past the dead letters it leaves alone,
		// and a group can't leave gaps, so each filter keeps its own position.
		g = "dlq-redrive-" + *topic
		if *errSub != "" {
			g += "-" + slug(*errSub)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := Run(ctx, Config{
		Brokers: brokers, Topic: *topic, Group: g, DryRun: *dryRun, Max: *max, ErrorContains: *errSub, Idle: *idle,
	})
	verb := "redriven"
	if *dryRun {
		verb = "would be redriven"
	}
	fmt.Printf("%d message(s) %s, %d skipped\n", res.Redriven, verb, res.Skipped)
	if err != nil {
		log.Fatal(err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// slug makes a filter text usable in a consumer group name.
func slug(s string) string {
	var b strings.Builder
	dash := true // so leading punctuation adds no dash
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			dash = false
		case !dash:
			b.WriteByte('-')
			dash = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}
