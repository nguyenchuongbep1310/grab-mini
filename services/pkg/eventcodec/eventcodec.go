// Package eventcodec encodes and decodes Kafka message values as Avro in
// Confluent's Schema Registry wire format:
//
//	byte 0      magic byte 0
//	bytes 1-4   schema id in the registry (big endian)
//	bytes 5...  the Avro binary payload
//
// A producer registers its schema under the subject "<topic>-value" the
// first time it sends to a topic; the registry rejects a schema that isn't
// compatible with the versions already there. A consumer reads the writer's
// schema id from each message, fetches that schema, and resolves it against
// its own (reader) schema, which is what lets producers and consumers be
// upgraded independently, e.g. after a field with a default is added.
package eventcodec

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"grab-mini/pkg/events"
	"grab-mini/pkg/secret"
)

const (
	magicByte   = 0
	contentType = "application/vnd.schemaregistry.v1+json"

	// MaxResponseBytes bounds what is read from the registry. A schema is a
	// few KB; a larger body is a misbehaving (or impersonated) registry, and
	// reading it unbounded would let it exhaust this process's memory.
	MaxResponseBytes = 1 << 20
)

// DecodeError means a message can never be decoded by this consumer (not
// Avro, corrupt, or written with a schema this reader can't resolve).
// Retrying it is pointless, and it says so via Permanent(), which
// kafkaconsumer uses to skip straight to the dead-letter topic. Failures
// talking to the registry are not DecodeErrors: those may clear up.
type DecodeError struct{ Err error }

func (e *DecodeError) Error() string   { return e.Err.Error() }
func (e *DecodeError) Unwrap() error   { return e.Err }
func (e *DecodeError) Permanent() bool { return true }

type Codec struct {
	baseURL   string
	client    *http.Client
	user      string
	password  string
	configErr error // a bad registry setting: returned by every call, never hidden
	schemaFor func(topic string) (string, error)

	mu       sync.Mutex
	local    map[string]avro.Schema // topic -> our (reader) schema
	writerID map[string]int         // topic -> id our schema has in the registry
	remote   map[int]avro.Schema    // schema id -> schema a producer wrote with
	resolved map[resolveKey]avro.Schema
}

type resolveKey struct {
	topic string
	id    int
}

type Option func(*Codec)

// WithSchemaSource replaces where the local schema for a topic comes from
// (by default the .avsc files embedded in package events).
func WithSchemaSource(f func(topic string) (string, error)) Option {
	return func(c *Codec) { c.schemaFor = f }
}

// WithBasicAuth sends HTTP basic credentials with every registry request.
func WithBasicAuth(user, password string) Option {
	return func(c *Codec) { c.user, c.password = user, password }
}

// WithHTTPClient replaces the HTTP client (for example to trust a private CA).
func WithHTTPClient(h *http.Client) Option {
	return func(c *Codec) { c.client = h }
}

// New returns a Codec for the registry at registryURL. Unless options say
// otherwise it is configured from the environment:
//
//	SCHEMA_REGISTRY_USER, SCHEMA_REGISTRY_PASSWORD   basic auth (each also
//	    readable from NAME_FILE); both unset means no authentication, which is
//	    what the local development registry wants
//	SCHEMA_REGISTRY_CA_FILE   PEM CA bundle to trust for an https registry
//
// A bad setting (unreadable file, password without user, credentials over
// plain http when APP_ENV=production) is reported by the first call that
// would use the registry, never silently ignored.
func New(registryURL string, opts ...Option) *Codec {
	c := &Codec{
		baseURL:   registryURL,
		client:    &http.Client{Timeout: 10 * time.Second},
		schemaFor: events.Schema,
		local:     map[string]avro.Schema{},
		writerID:  map[string]int{},
		remote:    map[int]avro.Schema{},
		resolved:  map[resolveKey]avro.Schema{},
	}
	c.configErr = c.fromEnv()
	for _, opt := range opts {
		opt(c)
	}
	if c.configErr == nil && (c.user != "" || c.password != "") && secret.Production() {
		if u, err := url.Parse(c.baseURL); err != nil || u.Scheme != "https" {
			c.configErr = errors.New("refusing to send schema registry credentials over plain http when APP_ENV=production (use an https SCHEMA_REGISTRY_URL)")
		}
	}
	return c
}

func (c *Codec) fromEnv() error {
	user, err := optionalSecret("SCHEMA_REGISTRY_USER")
	if err != nil {
		return err
	}
	password, err := optionalSecret("SCHEMA_REGISTRY_PASSWORD")
	if err != nil {
		return err
	}
	if (user == "") != (password == "") {
		return errors.New("SCHEMA_REGISTRY_USER and SCHEMA_REGISTRY_PASSWORD must be set together")
	}
	c.user, c.password = user, password

	if path := os.Getenv("SCHEMA_REGISTRY_CA_FILE"); path != "" {
		pem, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("SCHEMA_REGISTRY_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("SCHEMA_REGISTRY_CA_FILE (%s) holds no PEM certificate", path)
		}
		c.client.Transport = &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		}
	}
	return nil
}

// optionalSecret is secret.Get where "not set" is an empty string, not an error.
func optionalSecret(name string) (string, error) {
	v, err := secret.Get(name)
	if err != nil && !errors.Is(err, secret.ErrMissing) {
		return "", err
	}
	return strings.TrimSpace(v), nil
}

// Encode serializes v with the topic's schema, registering the schema first
// if this codec hasn't yet.
func (c *Codec) Encode(ctx context.Context, topic string, v any) ([]byte, error) {
	schema, err := c.localSchema(topic)
	if err != nil {
		return nil, err
	}
	id, err := c.register(ctx, topic)
	if err != nil {
		return nil, err
	}
	payload, err := avro.Marshal(schema, v)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", topic, err)
	}

	out := make([]byte, 5, 5+len(payload))
	out[0] = magicByte
	binary.BigEndian.PutUint32(out[1:5], uint32(id))
	return append(out, payload...), nil
}

// Decode deserializes a message produced by Encode (by any version of the
// producer's schema) into v.
func (c *Codec) Decode(ctx context.Context, topic string, data []byte, v any) error {
	if len(data) < 5 || data[0] != magicByte {
		return &DecodeError{errors.New("not a Schema Registry Avro message (missing magic byte)")}
	}
	id := int(binary.BigEndian.Uint32(data[1:5]))

	schema, err := c.readerFor(ctx, topic, id)
	if err != nil {
		return err
	}
	if err := avro.Unmarshal(schema, data[5:], v); err != nil {
		return &DecodeError{fmt.Errorf("decode %s (schema id %d): %w", topic, id, err)}
	}
	return nil
}

func (c *Codec) localSchema(topic string) (avro.Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.local[topic]; ok {
		return s, nil
	}
	text, err := c.schemaFor(topic)
	if err != nil {
		return nil, err
	}
	s, err := avro.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse schema for %s: %w", topic, err)
	}
	c.local[topic] = s
	return s, nil
}

func (c *Codec) register(ctx context.Context, topic string) (int, error) {
	c.mu.Lock()
	id, ok := c.writerID[topic]
	c.mu.Unlock()
	if ok {
		return id, nil
	}

	text, err := c.schemaFor(topic)
	if err != nil {
		return 0, err
	}
	body, _ := json.Marshal(map[string]string{"schema": text, "schemaType": "AVRO"})
	var resp struct {
		ID int `json:"id"`
	}
	if err := c.call(ctx, http.MethodPost, "/subjects/"+events.Subject(topic)+"/versions", body, &resp); err != nil {
		return 0, fmt.Errorf("register schema for %s: %w", topic, err)
	}

	c.mu.Lock()
	c.writerID[topic] = resp.ID
	c.mu.Unlock()
	return resp.ID, nil
}

// readerFor returns the schema to decode messages written with schema id
// into our local struct: the writer's schema resolved against ours.
func (c *Codec) readerFor(ctx context.Context, topic string, id int) (avro.Schema, error) {
	key := resolveKey{topic, id}
	c.mu.Lock()
	if s, ok := c.resolved[key]; ok {
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()

	reader, err := c.localSchema(topic)
	if err != nil {
		return nil, err
	}
	writer, err := c.writerSchema(ctx, id)
	if err != nil {
		return nil, err
	}
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	if err != nil {
		return nil, &DecodeError{fmt.Errorf("schema id %d isn't readable with the %s schema: %w", id, topic, err)}
	}

	c.mu.Lock()
	c.resolved[key] = resolved
	c.mu.Unlock()
	return resolved, nil
}

func (c *Codec) writerSchema(ctx context.Context, id int) (avro.Schema, error) {
	c.mu.Lock()
	s, ok := c.remote[id]
	c.mu.Unlock()
	if ok {
		return s, nil
	}

	var resp struct {
		Schema string `json:"schema"`
	}
	if err := c.call(ctx, http.MethodGet, fmt.Sprintf("/schemas/ids/%d", id), nil, &resp); err != nil {
		return nil, fmt.Errorf("fetch schema id %d: %w", id, err)
	}
	s, err := avro.Parse(resp.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema id %d: %w", id, err)
	}

	c.mu.Lock()
	c.remote[id] = s
	c.mu.Unlock()
	return s, nil
}

func (c *Codec) call(ctx context.Context, method, path string, body []byte, out any) error {
	if c.configErr != nil {
		return c.configErr
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", contentType)
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, MaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read schema registry response: %w", err)
	}
	if len(raw) > MaxResponseBytes {
		return fmt.Errorf("schema registry response (status %d) is larger than %d bytes", res.StatusCode, MaxResponseBytes)
	}

	if res.StatusCode >= 300 {
		var apiErr struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Message != "" {
			return fmt.Errorf("schema registry %d: %s", res.StatusCode, apiErr.Message)
		}
		return fmt.Errorf("schema registry %d: %s", res.StatusCode, bytes.TrimSpace(raw))
	}
	return json.Unmarshal(raw, out)
}
