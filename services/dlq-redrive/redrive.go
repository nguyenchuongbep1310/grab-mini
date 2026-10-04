package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/kafkaconsumer"
)

// Config is one redrive.
type Config struct {
	Brokers []string
	Topic   string // the topic whose dead letters to redrive (the original, not ".DLQ")
	// Group remembers how far the redrive got, so running it again picks up
	// after the last message it handled instead of redriving everything twice.
	Group string
	// DryRun lists what would be redriven and changes nothing, not even the
	// group's position.
	DryRun bool
	// Max stops after this many messages are redriven (0: no limit).
	Max int
	// ErrorContains redrives only dead letters whose error contains this text,
	// so one cause can be fixed and redriven while another is still open.
	ErrorContains string
	// Idle is how long to wait for another dead letter before deciding the
	// topic is drained (default 3s).
	Idle time.Duration
	// Security overrides the environment's (see package kafkaconn).
	Security *kafkaconn.Security
}

// Result says what a redrive did.
type Result struct {
	Redriven int // messages published back to the original topic
	Skipped  int // dead letters left alone (filtered out, or not a dead letter)
}

const (
	headerRedrivenFrom = "x-redriven-from"
	headerRedriveOf    = "x-redrive-of"
)

// Run reads the dead-letter topic of cfg.Topic and republishes each message,
// with its original key and bytes, to cfg.Topic, then commits its position.
//
// A message is published before its dead letter is marked done, so a crash can
// redrive one twice but never lose one; the consumers of the original topic
// are idempotent (that is what lets them retry at all). A redriven message
// that fails again simply lands on the dead-letter topic again, as a new dead
// letter, for the next redrive.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Topic == "" || strings.HasSuffix(cfg.Topic, ".DLQ") {
		return Result{}, errors.New("give the original topic (for example order.created), not its .DLQ")
	}
	if cfg.Idle <= 0 {
		cfg.Idle = 3 * time.Second
	}
	sec := cfg.Security
	if sec == nil {
		sec = kafkaconn.Default()
	}
	dlq := events.DeadLetterTopic(cfg.Topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers, Topic: dlq, GroupID: cfg.Group,
		MaxWait: 500 * time.Millisecond, Dialer: sec.Dialer(),
		// A group with no committed position starts at the beginning: the
		// first redrive handles everything that is there.
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr: kafka.TCP(cfg.Brokers...), Topic: cfg.Topic, Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, Transport: sec.Transport(),
	}
	defer writer.Close()

	var res Result
	for cfg.Max == 0 || res.Redriven < cfg.Max {
		fetchCtx, cancel := context.WithTimeout(ctx, cfg.Idle)
		msg, err := reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				return res, nil // nothing more arrived: drained
			}
			return res, fmt.Errorf("read %s: %w", dlq, err)
		}

		var dead kafkaconsumer.DeadLetter
		if err := json.Unmarshal(msg.Value, &dead); err != nil || len(dead.ValueBase64) == 0 {
			log.Printf("skipping %s offset %d: not a dead letter this tool understands", dlq, msg.Offset)
			res.Skipped++
			if !cfg.DryRun {
				if err := reader.CommitMessages(ctx, msg); err != nil {
					return res, err
				}
			}
			continue
		}
		if cfg.ErrorContains != "" && !strings.Contains(dead.Error, cfg.ErrorContains) {
			res.Skipped++
			if !cfg.DryRun {
				if err := reader.CommitMessages(ctx, msg); err != nil {
					return res, err
				}
			}
			continue
		}

		if cfg.DryRun {
			fmt.Printf("would redrive key=%s offset=%d error=%q\n", msg.Key, msg.Offset, dead.Error)
			res.Redriven++
			continue
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   msg.Key,
			Value: dead.ValueBase64,
			Headers: []kafka.Header{
				{Key: headerRedrivenFrom, Value: []byte(dlq)},
				{Key: headerRedriveOf, Value: []byte(fmt.Sprintf("%d/%d", msg.Partition, msg.Offset))},
			},
		})
		if err != nil {
			return res, fmt.Errorf("republish to %s: %w", cfg.Topic, err)
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return res, fmt.Errorf("remember offset %d of %s: %w", msg.Offset, dlq, err)
		}
		res.Redriven++
	}
	return res, nil
}
