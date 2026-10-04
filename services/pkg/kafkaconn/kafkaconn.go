// Package kafkaconn makes every connection to Kafka encrypted and
// authenticated when the environment says so, in one place instead of in every
// service.
//
// Configuration (all optional; the default is the local, unauthenticated
// PLAINTEXT cluster):
//
//	KAFKA_SECURITY_PROTOCOL  PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
//	KAFKA_SASL_MECHANISM     SCRAM-SHA-512 (default) | SCRAM-SHA-256 | PLAIN
//	KAFKA_USERNAME           the service's Kafka principal
//	KAFKA_PASSWORD           its password; also read from KAFKA_PASSWORD_FILE
//	KAFKA_CA_FILE            PEM file of the CA that signed the brokers'
//	                         certificates; omit to use the system roots
//
// Each service gets its own principal, and the brokers' ACLs
// (scripts/kafka-acls.sh) let it do only what that service does.
package kafkaconn

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"grab-mini/pkg/secret"
)

// Security is how to talk to a broker. A nil *Security means plaintext.
type Security struct {
	TLS  *tls.Config
	SASL sasl.Mechanism
}

// Options are what FromEnv reads, spelled out so tests can build a Security
// without touching the environment.
type Options struct {
	Protocol  string // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	Mechanism string // SCRAM-SHA-512, SCRAM-SHA-256, PLAIN
	Username  string
	Password  string
	CAFile    string
}

// New builds a Security from options. It returns nil for PLAINTEXT.
func New(o Options) (*Security, error) {
	protocol := strings.ToUpper(strings.TrimSpace(o.Protocol))
	if protocol == "" {
		protocol = "PLAINTEXT"
	}
	useTLS := protocol == "SSL" || protocol == "SASL_SSL"
	useSASL := protocol == "SASL_PLAINTEXT" || protocol == "SASL_SSL"
	if protocol != "PLAINTEXT" && !useTLS && !useSASL {
		return nil, fmt.Errorf("KAFKA_SECURITY_PROTOCOL %q: want PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL", o.Protocol)
	}
	if protocol == "PLAINTEXT" {
		return nil, nil
	}

	s := &Security{}
	if useTLS {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if o.CAFile != "" {
			pem, err := os.ReadFile(o.CAFile)
			if err != nil {
				return nil, fmt.Errorf("KAFKA_CA_FILE: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("KAFKA_CA_FILE %s holds no PEM certificate", o.CAFile)
			}
			cfg.RootCAs = pool
		}
		s.TLS = cfg
	}
	if useSASL {
		if o.Username == "" || o.Password == "" {
			return nil, errors.New("SASL needs KAFKA_USERNAME and KAFKA_PASSWORD")
		}
		mech, err := mechanism(o.Mechanism, o.Username, o.Password)
		if err != nil {
			return nil, err
		}
		s.SASL = mech
	}
	return s, nil
}

func mechanism(name, user, pass string) (sasl.Mechanism, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, user, pass)
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, user, pass)
	case "PLAIN":
		return plain.Mechanism{Username: user, Password: pass}, nil
	}
	return nil, fmt.Errorf("KAFKA_SASL_MECHANISM %q: want SCRAM-SHA-512, SCRAM-SHA-256 or PLAIN", name)
}

// FromEnv reads the KAFKA_* variables. In production it refuses an
// unencrypted, unauthenticated connection unless KAFKA_ALLOW_PLAINTEXT=true.
func FromEnv() (*Security, error) {
	o := Options{
		Protocol:  os.Getenv("KAFKA_SECURITY_PROTOCOL"),
		Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		Username:  os.Getenv("KAFKA_USERNAME"),
		CAFile:    os.Getenv("KAFKA_CA_FILE"),
	}
	if strings.HasPrefix(strings.ToUpper(o.Protocol), "SASL") {
		p, err := secret.Get("KAFKA_PASSWORD")
		if err != nil {
			return nil, err
		}
		o.Password = p
	}
	s, err := New(o)
	if err != nil {
		return nil, err
	}
	if s == nil && secret.Production() && !strings.EqualFold(os.Getenv("KAFKA_ALLOW_PLAINTEXT"), "true") {
		return nil, errors.New("refusing to connect to Kafka without TLS and authentication when APP_ENV=production " +
			"(set KAFKA_SECURITY_PROTOCOL=SASL_SSL, or KAFKA_ALLOW_PLAINTEXT=true to override)")
	}
	return s, nil
}

var (
	once       sync.Once
	defaultSec *Security
)

// Default is FromEnv, read once; a misconfiguration stops the process at
// start-up rather than failing later on some connection.
func Default() *Security {
	once.Do(func() {
		s, err := FromEnv()
		if err != nil {
			log.Fatalf("kafka: %v", err)
		}
		defaultSec = s
		if s != nil {
			log.Printf("kafka: connecting with TLS=%v SASL=%v", s.TLS != nil, s.SASL != nil)
		}
	})
	return defaultSec
}

const dialTimeout = 10 * time.Second

// Dialer is for kafka.Reader (ReaderConfig.Dialer). Nil-safe.
func (s *Security) Dialer() *kafka.Dialer {
	d := &kafka.Dialer{Timeout: dialTimeout, DualStack: true}
	if s != nil {
		d.TLS = s.TLS
		d.SASLMechanism = s.SASL
	}
	return d
}

// Transport is for kafka.Writer and kafka.Client. Nil-safe.
func (s *Security) Transport() *kafka.Transport {
	t := &kafka.Transport{DialTimeout: dialTimeout, IdleTimeout: 30 * time.Second}
	if s != nil {
		t.TLS = s.TLS
		t.SASL = s.SASL
	}
	return t
}

// DefaultBatchTimeout is how long a Writer built by Writer holds a message to
// fill a batch. kafka-go's own default is **one second**, and WriteMessages
// waits for it: every synchronous publish then takes at least a second. Found
// by load testing: with the default, an order took over a second per hop (the
// outbox relay, Matching, the retry topics) and the 3 s matching SLO could not
// be met at any load. 10 ms still batches under load (a flush also happens when
// the batch is full), and costs a single publish almost nothing. 2 ms rather
// than 10 because load testing showed Matching (one publish per message, with
// 12 workers) bounded by the time one message takes: 10 ms of waiting was a
// third of it.
const DefaultBatchTimeout = 2 * time.Millisecond

// Writer applies the default security to w, and a batch timeout fit for
// synchronous publishing if w has none, and returns it.
func Writer(w *kafka.Writer) *kafka.Writer {
	w.Transport = Default().Transport()
	if w.BatchTimeout == 0 {
		w.BatchTimeout = DefaultBatchTimeout
	}
	return w
}

// Client is a kafka.Client for brokers using the default security.
func Client(brokers []string, timeout time.Duration) *kafka.Client {
	return &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: timeout, Transport: Default().Transport()}
}
