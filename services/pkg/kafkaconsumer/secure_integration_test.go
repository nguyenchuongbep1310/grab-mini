package kafkaconsumer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconn"
)

// Against a broker with TLS, SASL/SCRAM and ACLs, set up by scripts/kafka-acls.sh:
//
//	TEST_SECURE_KAFKA_BROKER   host:port of the SASL_SSL listener
//	TEST_SECURE_KAFKA_CA       PEM file of the CA that signed the broker's certificate
//	TEST_SECURE_KAFKA_SECRETS  the SECRETS_DIR the script wrote (<service>.password)
//	TEST_SECURE_KAFKA_ADMIN    the super user's password (the user is "admin")
//
// They check both directions of least privilege: a service can do everything
// its code does (including a consumer's whole retry and dead-letter path), and
// cannot do what it shouldn't.

type secureEnv struct {
	brokers []string
	ca      string
	secrets string
	admin   string
}

func secureKafka(t *testing.T) secureEnv {
	t.Helper()
	e := secureEnv{
		brokers: []string{os.Getenv("TEST_SECURE_KAFKA_BROKER")},
		ca:      os.Getenv("TEST_SECURE_KAFKA_CA"),
		secrets: os.Getenv("TEST_SECURE_KAFKA_SECRETS"),
		admin:   os.Getenv("TEST_SECURE_KAFKA_ADMIN"),
	}
	if e.brokers[0] == "" || e.ca == "" || e.secrets == "" || e.admin == "" {
		t.Skip("TEST_SECURE_KAFKA_* not set")
	}
	return e
}

func (e secureEnv) as(t *testing.T, user, password string) *kafkaconn.Security {
	t.Helper()
	s, err := kafkaconn.New(kafkaconn.Options{Protocol: "SASL_SSL", Username: user, Password: password, CAFile: e.ca})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e secureEnv) service(t *testing.T, name string) *kafkaconn.Security {
	t.Helper()
	pw, err := os.ReadFile(filepath.Join(e.secrets, name+".password"))
	if err != nil {
		t.Fatalf("run scripts/kafka-acls.sh first: %v", err)
	}
	return e.as(t, name, strings.TrimSpace(string(pw)))
}

func (e secureEnv) writer(s *kafkaconn.Security, topic string) *kafka.Writer {
	return &kafka.Writer{Addr: kafka.TCP(e.brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, Transport: s.Transport()}
}

// ensureTopics creates the base topic as the admin, the way the deployment's
// topic-creation step would. Its retry and dead-letter topics are left for the
// consumer to create (the broker doesn't auto-create topics).
func (e secureEnv) ensureTopics(t *testing.T, topic string) {
	t.Helper()
	client := &kafka.Client{Addr: kafka.TCP(e.brokers...), Transport: e.as(t, "admin", e.admin).Transport()}
	resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range resp.Errors {
		if e != nil && !errors.Is(e, kafka.TopicAlreadyExists) {
			t.Fatalf("create %s: %v", name, e)
		}
	}
}

func TestAMatchingServiceCanRunItsWholeConsumerPathUnderItsACLs(t *testing.T) {
	e := secureKafka(t)
	e.ensureTopics(t, events.TopicOrderCreated)
	tag := uuid.NewString()[:8]

	var calls, recovered = make(chan string, 50), make(chan struct{}, 1)
	failures := map[string]int{"flaky-" + tag: 2, "doomed-" + tag: 1 << 30}
	cfg := Config{
		Brokers: e.brokers, Topic: events.TopicOrderCreated,
		GroupID:    "matching-service-group-test-" + tag, // inside the group prefix the ACL grants
		Security:   e.service(t, "matching-service"),
		RetryDelay: 10 * time.Millisecond, RetryTiers: []time.Duration{200 * time.Millisecond, 200 * time.Millisecond},
		Partitions: 1, LogLabel: "[secure-test]",
	}
	start(t, cfg, func(_ context.Context, msg kafka.Message) error {
		v := string(msg.Value)
		n, ours := failures[v]
		if !ours {
			return nil // messages left by other runs
		}
		calls <- v
		if v == "flaky-"+tag && n > 0 {
			failures[v]--
			if failures[v] == 0 {
				recovered <- struct{}{}
			}
		}
		if failures[v] > 0 || v == "doomed-"+tag {
			return errors.New("not yet")
		}
		return nil
	})

	admin := e.writer(e.as(t, "admin", e.admin), events.TopicOrderCreated)
	defer admin.Close()
	for _, v := range []string{"flaky-" + tag, "doomed-" + tag} {
		if err := admin.WriteMessages(context.Background(), kafka.Message{Key: []byte(v), Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}

	// The flaky one fails twice (in place, then once from a retry topic) and then succeeds.
	select {
	case <-recovered:
	case <-time.After(20 * time.Second):
		t.Fatal("the retry path did not work under the service's ACLs (the service must be able to create, write and read its retry topics)")
	}

	// The doomed one ends on the dead-letter topic, which the service may write.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: e.brokers, Topic: events.DeadLetterTopic(events.TopicOrderCreated), Partition: 0, MaxWait: 200 * time.Millisecond,
		Dialer: e.as(t, "admin", e.admin).Dialer(),
	})
	defer r.Close()
	r.SetOffset(kafka.FirstOffset)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("the dead-lettered message never arrived: %v", err)
		}
		if strings.Contains(string(m.Value), "doomed-"+tag) {
			return
		}
	}
}

func TestAServiceCannotProduceToATopicItDoesNotOwn(t *testing.T) {
	e := secureKafka(t)
	e.ensureTopics(t, events.TopicOrderMatched)
	e.ensureTopics(t, events.TopicOrderCreated)
	svc := e.service(t, "order-service")

	w := e.writer(svc, events.TopicOrderMatched)
	defer w.Close()
	err := w.WriteMessages(context.Background(), kafka.Message{Value: []byte("forged match")})
	// kafka-go wraps the broker's error in a WriteErrors, so look at its text.
	if err == nil || !strings.Contains(err.Error(), "Topic Authorization Failed") {
		t.Errorf("order-service writing order.matched: %v, want Topic Authorization Failed", err)
	}

	own := e.writer(svc, events.TopicOrderCreated)
	defer own.Close()
	if err := own.WriteMessages(context.Background(), kafka.Message{Value: []byte("a real order")}); err != nil {
		t.Errorf("order-service writing its own topic: %v", err)
	}
}

func TestAServiceCannotReadATopicItDoesNotConsume(t *testing.T) {
	e := secureKafka(t)
	e.ensureTopics(t, events.TopicOrderCreated)
	// rating-service consumes only trip.rated.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: e.brokers, Topic: events.TopicOrderCreated, GroupID: "rating-service-group-test-" + uuid.NewString()[:8],
		Dialer: e.service(t, "rating-service").Dialer(), MaxWait: 200 * time.Millisecond,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if m, err := r.FetchMessage(ctx); err == nil {
		t.Errorf("rating-service read order.created: %s", m.Value)
	}
}

func TestBadCredentialsAndAnUntrustedBrokerAreRefused(t *testing.T) {
	e := secureKafka(t)
	check := func(name string, s *kafkaconn.Security) {
		client := &kafka.Client{Addr: kafka.TCP(e.brokers...), Timeout: 5 * time.Second, Transport: s.Transport()}
		if _, err := client.Metadata(context.Background(), &kafka.MetadataRequest{}); err == nil {
			t.Errorf("%s: the broker accepted the connection", name)
		}
	}
	check("wrong password", e.as(t, "order-service", "not-the-password"))
	check("unknown user", e.as(t, "mallory", "whatever"))

	noCA, err := kafkaconn.New(kafkaconn.Options{Protocol: "SASL_SSL", Username: "admin", Password: e.admin}) // system roots only
	if err != nil {
		t.Fatal(err)
	}
	check("a broker certificate the client doesn't trust", noCA)

	// And the same client, correctly configured, is let in.
	client := &kafka.Client{Addr: kafka.TCP(e.brokers...), Timeout: 5 * time.Second, Transport: e.as(t, "admin", e.admin).Transport()}
	if _, err := client.Metadata(context.Background(), &kafka.MetadataRequest{}); err != nil {
		t.Errorf("a correctly configured client was refused: %v", err)
	}
}
