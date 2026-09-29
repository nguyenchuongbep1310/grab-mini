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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"

	"grab-mini/pkg/events"
)

const (
	magicByte   = 0
	contentType = "application/vnd.schemaregistry.v1+json"
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
	for _, opt := range opts {
		opt(c)
	}
	return c
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
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", contentType)

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)

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
