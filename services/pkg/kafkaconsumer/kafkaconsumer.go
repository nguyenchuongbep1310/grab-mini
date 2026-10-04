// Package kafkaconsumer is the retry + dead-letter-topic consumer loop
// shared by every service in this project, so each one doesn't hand-roll
// its own fetch/retry/commit logic (US-6.2, task 5.1/5.3).
package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"
	"unicode/utf8"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
)

// permanent is implemented by errors that retrying can't fix (see
// eventcodec.DecodeError). A handler returning one skips the retries.
type permanent interface{ Permanent() bool }

func isPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p) && p.Permanent()
}

// Handler processes one Kafka message. Returning an error triggers a
// retry, and eventually (after MaxRetries) publishing to the topic's
// dead-letter topic.
type Handler func(ctx context.Context, msg kafka.Message) error

type Config struct {
	Brokers []string
	GroupID string

	// Topic is for a consumer subscribed to a single topic. Topics is
	// for a consumer subscribed to several topics under one group (set
	// exactly one of the two).
	Topic  string
	Topics []string

	// MaxRetries is how many times a failed message is retried before
	// giving up on it (default 3). RetryDelay is the pause between
	// attempts (default 2s).
	MaxRetries int
	RetryDelay time.Duration

	// LogLabel prefixes log lines, e.g. "[matching-A]"; defaults to GroupID.
	LogLabel string
}

// Run fetches messages for cfg until ctx is cancelled, retrying each one
// that fails and, once retries are exhausted, publishing it — with the
// failure reason attached — to <topic>.DLQ instead of the service crashing
// or getting stuck on it (US-6.2). Every message logs its
// topic/partition/offset (task 5.3), and every message is committed
// exactly once handling is done (success or given up), so one poison
// message never blocks the rest of the partition.
//
// Cancelling ctx is a graceful shutdown: a message already handled is still
// committed, one interrupted mid-retry is left uncommitted so it is
// redelivered, and Run returns after closing its reader and writer. A fetch
// error that isn't a shutdown is retried with a growing pause instead of
// ending the process.
func Run(ctx context.Context, cfg Config, handle Handler) {
	label := cfg.LogLabel
	if label == "" {
		label = cfg.GroupID
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	retryDelay := cfg.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 2 * time.Second
	}

	readerCfg := kafka.ReaderConfig{Brokers: cfg.Brokers, GroupID: cfg.GroupID}
	if cfg.Topic != "" {
		readerCfg.Topic = cfg.Topic
	} else {
		readerCfg.GroupTopics = cfg.Topics
	}
	reader := kafka.NewReader(readerCfg)
	defer reader.Close()

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer dlqWriter.Close()

	log.Printf("%s consumer running", label)

	fetchBackoff := time.Duration(0)
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				log.Printf("%s consumer stopped", label)
				return
			}
			fetchBackoff = min(max(fetchBackoff*2, time.Second), 30*time.Second)
			log.Printf("%s failed to fetch message, retrying in %s: %v", label, fetchBackoff, err)
			if !sleep(ctx, fetchBackoff) {
				return
			}
			continue
		}
		fetchBackoff = 0

		log.Printf("%s received topic=%s partition=%d offset=%d key=%s", label, msg.Topic, msg.Partition, msg.Offset, string(msg.Key))

		handleErr := handleWithRetry(ctx, handle, msg, maxRetries, retryDelay, label)
		if ctx.Err() != nil && handleErr != nil {
			// Shutting down mid-message: don't dead-letter or commit it,
			// so the next instance handles it from the start.
			log.Printf("%s stopped while handling topic=%s partition=%d offset=%d, leaving it uncommitted", label, msg.Topic, msg.Partition, msg.Offset)
			return
		}

		// Finishing a message must not depend on ctx, or a shutdown that
		// lands right after a successful handle would redeliver it.
		doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if handleErr != nil {
			publishToDLQ(doneCtx, dlqWriter, msg, handleErr, label)
		}
		if err := reader.CommitMessages(doneCtx, msg); err != nil {
			log.Printf("%s failed to commit offset: %v", label, err)
		}
		cancel()
	}
}

// handleWithRetry runs handle until it succeeds, fails permanently, runs out
// of retries, or ctx is cancelled. It returns the last error (nil on success).
func handleWithRetry(ctx context.Context, handle Handler, msg kafka.Message, maxRetries int, retryDelay time.Duration, label string) error {
	var handleErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 && !sleep(ctx, retryDelay) {
			return ctx.Err()
		}
		handleErr = handle(ctx, msg)
		if handleErr == nil {
			return nil
		}
		log.Printf("%s attempt %d/%d failed topic=%s partition=%d offset=%d: %v",
			label, attempt+1, maxRetries+1, msg.Topic, msg.Partition, msg.Offset, handleErr)
		if isPermanent(handleErr) {
			log.Printf("%s not retrying: the error is permanent", label)
			return handleErr
		}
	}
	return handleErr
}

// sleep waits d, and reports false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// dlqEnvelope wraps a message that couldn't be processed. Values are Avro
// (binary), not JSON, so the original bytes travel base64-encoded (that's
// what encoding/json does for []byte); valueText is a readable copy when
// the bytes happen to be text, e.g. a hand-published JSON message.
type dlqEnvelope struct {
	OriginalTopic     string `json:"originalTopic"`
	OriginalPartition int    `json:"originalPartition"`
	OriginalOffset    int64  `json:"originalOffset"`
	Error             string `json:"error"`
	ValueBase64       []byte `json:"valueBase64"`
	ValueText         string `json:"valueText,omitempty"`
}

func printable(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		if r < 0x20 && r != '\n' && r != '\t' {
			return false
		}
	}
	return true
}

func publishToDLQ(ctx context.Context, w *kafka.Writer, msg kafka.Message, cause error, label string) {
	dlqTopic := events.DeadLetterTopic(msg.Topic)

	envelope := dlqEnvelope{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		Error:             cause.Error(),
		ValueBase64:       msg.Value,
	}
	if printable(msg.Value) {
		envelope.ValueText = string(msg.Value)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("%s failed to marshal DLQ envelope: %v", label, err)
		return
	}

	// The first write to a not-yet-existing DLQ topic can itself fail
	// even with AllowAutoTopicCreation: the write that triggers creation
	// is the one that errors, and the topic only becomes writable after.
	// One retry covers that one-time race per topic.
	dlqMsg := kafka.Message{Topic: dlqTopic, Key: msg.Key, Value: payload}
	err = w.WriteMessages(ctx, dlqMsg)
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		err = w.WriteMessages(ctx, dlqMsg)
	}
	if err != nil {
		log.Printf("%s failed to publish to %s: %v", label, dlqTopic, err)
		return
	}
	log.Printf("%s giving up on the message, published to %s (partition=%d offset=%d)", label, dlqTopic, msg.Partition, msg.Offset)
}
