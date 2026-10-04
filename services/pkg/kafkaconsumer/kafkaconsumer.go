// Package kafkaconsumer is the retry + dead-letter-topic consumer loop
// shared by every service in this project, so each one doesn't hand-roll
// its own fetch/retry/commit logic (US-6.2, task 5.1/5.3).
//
// A message that fails is handled in one of three ways:
//
//   - the error is permanent (see eventcodec.DecodeError): straight to
//     <topic>.DLQ, retrying can't help;
//   - the error is transient and the config has RetryTiers: the message moves
//     to <topic>.retry.1 and the partition carries on. A reader of that topic
//     waits until the message is due (the tier's delay), tries again, and on
//     failure moves it to the next tier, and after the last tier to the DLQ;
//   - the error is transient and there are no RetryTiers: retried in place a
//     few times, which holds up the partition while it waits.
//
// Moving a message to a retry topic is what keeps a slow dependency (a
// database that is down for a minute) from stalling every message behind it.
// The price is ordering: a retried message is handled after the ones that
// were behind it, so a handler used with RetryTiers must cope with events
// out of order (trip-service and order-service do: state changes are guarded
// by the current status, and every event carries an eventId).
package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconn"
)

// permanent is implemented by errors that retrying can't fix (see
// eventcodec.DecodeError). A handler returning one skips the retries.
type permanent interface{ Permanent() bool }

func isPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p) && p.Permanent()
}

// Handler processes one Kafka message. Returning an error triggers a
// retry, and eventually publishing to the topic's dead-letter topic.
// msg.Topic is always the topic the message was first published to, even
// when it arrives from a retry topic, so a handler can decode by topic.
type Handler func(ctx context.Context, msg kafka.Message) error

// DefaultRetryTiers waits 5 seconds, then 1 minute, then 10 minutes before
// giving up on a message.
var DefaultRetryTiers = []time.Duration{5 * time.Second, time.Minute, 10 * time.Minute}

type Config struct {
	Brokers []string
	GroupID string

	// Topic is for a consumer subscribed to a single topic. Topics is
	// for a consumer subscribed to several topics under one group (set
	// exactly one of the two).
	Topic  string
	Topics []string

	// RetryTiers turns on retry topics: the delay before each successive
	// retry (see the package comment). Leave it empty to retry in place.
	RetryTiers []time.Duration

	// MaxRetries is how many times a failed message is retried in place
	// before the next step (default 3 without RetryTiers, 1 with them: a
	// quick second try rides out a blip without a trip through a retry
	// topic). RetryDelay is the pause between those attempts (default 2s).
	MaxRetries int
	RetryDelay time.Duration

	// Security overrides the connection security read from the environment
	// (see package kafkaconn); nil means use that. For tests.
	Security *kafkaconn.Security

	// Retry and dead-letter topics are created on startup if missing (with the
	// Create permission the ACLs grant on "<topic>."), with
	// these settings (defaults 3 partitions, replication factor 1: right for
	// the local single-broker cluster; on a real cluster create them ahead
	// of time and these are left alone).
	Partitions        int
	ReplicationFactor int

	// LogLabel prefixes log lines, e.g. "[matching-A]"; defaults to GroupID.
	LogLabel string
}

var (
	// result is "ok", "retried" (moved to a retry topic) or "dead_lettered".
	// A message is counted once per time it leaves a consumer: one that goes
	// through two retry topics and then succeeds is retried, retried, ok.
	messagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_total",
		Help: "Messages a consumer finished with, by group, original topic and outcome.",
	}, []string{"group", "topic", "result"})

	handleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_consumer_handle_duration_seconds",
		Help:    "Time a handler took on one attempt, by group and original topic.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"group", "topic"})
)

const (
	headerOriginalTopic = "x-original-topic"
	headerAttempt       = "x-retry-attempt"    // retry topics used so far
	headerNotBefore     = "x-retry-not-before" // unix milliseconds
	headerLastError     = "x-last-error"
)

type consumer struct {
	cfg        Config
	label      string
	maxRetries int
	retryDelay time.Duration
	topics     []string
	out        *kafka.Writer // retry and dead-letter topics
	security   *kafkaconn.Security
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
// redelivered, and Run returns after closing its readers and writer. A fetch
// error that isn't a shutdown is retried with a growing pause instead of
// ending the process.
func Run(ctx context.Context, cfg Config, handle Handler) {
	c := &consumer{cfg: cfg, label: cfg.LogLabel, maxRetries: cfg.MaxRetries, retryDelay: cfg.RetryDelay}
	if c.label == "" {
		c.label = cfg.GroupID
	}
	if c.maxRetries <= 0 {
		c.maxRetries = 3
		if len(cfg.RetryTiers) > 0 {
			c.maxRetries = 1
		}
	}
	if c.retryDelay <= 0 {
		c.retryDelay = 2 * time.Second
	}
	c.topics = cfg.Topics
	if cfg.Topic != "" {
		c.topics = []string{cfg.Topic}
	}

	c.security = cfg.Security
	if c.security == nil {
		c.security = kafkaconn.Default()
	}

	c.out = &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Transport:              c.security.Transport(),
	}
	defer c.out.Close()

	if err := c.ensureTopics(ctx); err != nil {
		log.Printf("%s could not create its retry and dead-letter topics (they must exist, or the broker must auto-create them): %v", c.label, err)
	}

	// Time every attempt, wherever it runs.
	timed := func(ctx context.Context, msg kafka.Message) error {
		start := time.Now()
		err := handle(ctx, msg)
		handleDuration.WithLabelValues(cfg.GroupID, msg.Topic).Observe(time.Since(start).Seconds())
		return err
	}

	var tiers sync.WaitGroup
	for i := range cfg.RetryTiers {
		tiers.Go(func() { c.runRetryTier(ctx, i+1, timed) })
	}
	c.runSource(ctx, timed)
	tiers.Wait()
}

func (c *consumer) newReader(groupID string, topics []string) *kafka.Reader {
	rc := kafka.ReaderConfig{
		Brokers: c.cfg.Brokers,
		GroupID: groupID,
		// MaxWait is how long a fetch waits for data when a topic is idle,
		// and Close waits for the fetch in flight: at the default of 10s a
		// service took up to 10s to stop. Half a second costs two empty
		// fetches a second per partition.
		MaxWait: 500 * time.Millisecond,
		Dialer:  c.security.Dialer(),
	}
	if len(topics) == 1 {
		rc.Topic = topics[0]
	} else {
		rc.GroupTopics = topics
	}
	return kafka.NewReader(rc)
}

// runSource consumes the topics the service subscribes to.
func (c *consumer) runSource(ctx context.Context, handle Handler) {
	reader := c.newReader(c.cfg.GroupID, c.topics)
	defer reader.Close()
	log.Printf("%s consumer running", c.label)

	c.loop(ctx, reader, func(ctx context.Context, msg kafka.Message) (kafka.Message, bool) {
		slog.Info("received", "consumer", c.label, "group", c.cfg.GroupID, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key))
		err := handleWithRetry(ctx, handle, msg, c.maxRetries, c.retryDelay, c.label)
		if err != nil && ctx.Err() != nil {
			return msg, false
		}
		if err != nil {
			c.fail(ctx, msg, err, 0)
		} else {
			messagesTotal.WithLabelValues(c.cfg.GroupID, msg.Topic, "ok").Inc()
		}
		return msg, true
	})
}

// runRetryTier consumes the tier'th retry topic of every subscribed topic.
func (c *consumer) runRetryTier(ctx context.Context, tier int, handle Handler) {
	var retryTopics []string
	for _, t := range c.topics {
		retryTopics = append(retryTopics, events.RetryTopic(t, tier))
	}
	reader := c.newReader(fmt.Sprintf("%s.retry.%d", c.cfg.GroupID, tier), retryTopics)
	defer reader.Close()
	label := fmt.Sprintf("%s retry-%d", c.label, tier)
	log.Printf("%s consumer running", label)

	c.loop(ctx, reader, func(ctx context.Context, msg kafka.Message) (kafka.Message, bool) {
		// Hand the handler the message as it was first published.
		msg.Topic = headerValue(msg, headerOriginalTopic, msg.Topic)

		if due := time.UnixMilli(headerInt(msg, headerNotBefore, 0)); time.Until(due) > 0 {
			if !sleep(ctx, time.Until(due)) {
				return msg, false
			}
		}
		slog.Info("retrying", "consumer", label, "group", c.cfg.GroupID, "topic", msg.Topic, "key", string(msg.Key), "retryPartition", msg.Partition, "retryOffset", msg.Offset)

		// One attempt: the wait above was the back-off.
		err := handle(ctx, msg)
		if err != nil && ctx.Err() != nil {
			return msg, false
		}
		if err != nil {
			c.fail(ctx, msg, err, tier)
		} else {
			messagesTotal.WithLabelValues(c.cfg.GroupID, msg.Topic, "ok").Inc()
		}
		return msg, true
	})
}

// loop fetches from reader and calls process for each message. process
// reports false when it was interrupted by shutdown, meaning the message
// must stay uncommitted; otherwise the message is committed.
func (c *consumer) loop(ctx context.Context, reader *kafka.Reader, process func(context.Context, kafka.Message) (kafka.Message, bool)) {
	fetchBackoff := time.Duration(0)
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				log.Printf("%s consumer stopped", c.label)
				return
			}
			fetchBackoff = min(max(fetchBackoff*2, time.Second), 30*time.Second)
			log.Printf("%s failed to fetch message, retrying in %s: %v", c.label, fetchBackoff, err)
			if !sleep(ctx, fetchBackoff) {
				return
			}
			continue
		}
		fetchBackoff = 0

		handled, ok := process(ctx, msg)
		if !ok {
			// Shutting down mid-message: don't dead-letter or commit it,
			// so the next instance handles it from the start.
			log.Printf("%s stopped while handling topic=%s partition=%d offset=%d, leaving it uncommitted", c.label, handled.Topic, msg.Partition, msg.Offset)
			return
		}

		// Finishing a message must not depend on ctx, or a shutdown that
		// lands right after a successful handle would redeliver it.
		doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if err := reader.CommitMessages(doneCtx, msg); err != nil {
			log.Printf("%s failed to commit offset: %v", c.label, err)
		}
		cancel()
	}
}

// fail moves a message that failed to its next stop: the next retry topic
// (attempt is how many it has already been through), or the dead-letter
// topic when the error is permanent or the tiers are used up. It must run
// before the message is committed.
func (c *consumer) fail(ctx context.Context, msg kafka.Message, cause error, attempt int) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if isPermanent(cause) || attempt >= len(c.cfg.RetryTiers) {
		messagesTotal.WithLabelValues(c.cfg.GroupID, msg.Topic, "dead_lettered").Inc()
		publishToDLQ(ctx, c.out, msg, cause, attempt, c.label)
		return
	}
	next := attempt + 1
	retry := kafka.Message{
		Topic: events.RetryTopic(msg.Topic, next),
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: headerOriginalTopic, Value: []byte(msg.Topic)},
			{Key: headerAttempt, Value: []byte(strconv.Itoa(next))},
			{Key: headerNotBefore, Value: []byte(strconv.FormatInt(time.Now().Add(c.cfg.RetryTiers[attempt]).UnixMilli(), 10))},
			{Key: headerLastError, Value: []byte(cause.Error())},
		},
	}
	if err := c.out.WriteMessages(ctx, retry); err != nil {
		log.Printf("%s failed to publish to %s, dead-lettering instead: %v", c.label, retry.Topic, err)
		messagesTotal.WithLabelValues(c.cfg.GroupID, msg.Topic, "dead_lettered").Inc()
		publishToDLQ(ctx, c.out, msg, cause, attempt, c.label)
		return
	}
	messagesTotal.WithLabelValues(c.cfg.GroupID, msg.Topic, "retried").Inc()
	slog.Warn("will retry", "consumer", c.label, "group", c.cfg.GroupID, "topic", msg.Topic, "key", string(msg.Key),
		"retryTopic", retry.Topic, "after", c.cfg.RetryTiers[attempt].String(), "error", cause.Error())
}

// ensureTopics creates the retry topics and the dead-letter topic of every
// subscribed topic that don't exist yet, so the failure path works on a broker
// that doesn't auto-create topics (production's shouldn't).
func (c *consumer) ensureTopics(ctx context.Context) error {
	partitions, rf := c.cfg.Partitions, c.cfg.ReplicationFactor
	if partitions <= 0 {
		partitions = 3
	}
	if rf <= 0 {
		rf = 1
	}
	var specs []kafka.TopicConfig
	for _, t := range c.topics {
		specs = append(specs, kafka.TopicConfig{Topic: events.DeadLetterTopic(t), NumPartitions: partitions, ReplicationFactor: rf})
		for tier := 1; tier <= len(c.cfg.RetryTiers); tier++ {
			specs = append(specs, kafka.TopicConfig{Topic: events.RetryTopic(t, tier), NumPartitions: partitions, ReplicationFactor: rf})
		}
	}
	client := &kafka.Client{Addr: kafka.TCP(c.cfg.Brokers...), Timeout: 10 * time.Second, Transport: c.security.Transport()}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: specs})
	if err != nil {
		return err
	}
	for topic, err := range resp.Errors {
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			return fmt.Errorf("create %s: %w", topic, err)
		}
	}
	return nil
}

func headerValue(msg kafka.Message, key, fallback string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return fallback
}

func headerInt(msg kafka.Message, key string, fallback int64) int64 {
	n, err := strconv.ParseInt(headerValue(msg, key, ""), 10, 64)
	if err != nil {
		return fallback
	}
	return n
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
	// Retries is how many retry topics the message went through first.
	Retries     int    `json:"retries"`
	ValueBase64 []byte `json:"valueBase64"`
	ValueText   string `json:"valueText,omitempty"`
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

func publishToDLQ(ctx context.Context, w *kafka.Writer, msg kafka.Message, cause error, retries int, label string) {
	dlqTopic := events.DeadLetterTopic(msg.Topic)

	envelope := dlqEnvelope{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		Error:             cause.Error(),
		Retries:           retries,
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
	slog.Error("message dead-lettered", "consumer", label, "dlqTopic", dlqTopic, "topic", msg.Topic,
		"partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "retries", retries, "error", cause.Error())
}
