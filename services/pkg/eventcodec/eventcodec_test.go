package eventcodec

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"grab-mini/pkg/events"
)

// fakeRegistry implements just the two Schema Registry endpoints the codec
// uses. Registering identical text twice returns the same id, like the real one.
type fakeRegistry struct {
	mu      sync.Mutex
	schemas []string
	rejects bool
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/subjects/"):
		if f.rejects {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{"error_code": 409, "message": "Schema being registered is incompatible with an earlier schema"})
			return
		}
		var body struct{ Schema string }
		json.NewDecoder(r.Body).Decode(&body)
		for i, s := range f.schemas {
			if s == body.Schema {
				json.NewEncoder(w).Encode(map[string]int{"id": i + 1})
				return
			}
		}
		f.schemas = append(f.schemas, body.Schema)
		json.NewEncoder(w).Encode(map[string]int{"id": len(f.schemas)})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/"):
		var id int
		fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/schemas/ids/"), "%d", &id)
		if id < 1 || id > len(f.schemas) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error_code": 40403, "message": "Schema not found"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"schema": f.schemas[id-1]})
	default:
		http.NotFound(w, r)
	}
}

func newRegistry(t *testing.T) (*fakeRegistry, string) {
	t.Helper()
	reg := &fakeRegistry{}
	srv := httptest.NewServer(reg)
	t.Cleanup(srv.Close)
	return reg, srv.URL
}

var sampleOrder = events.OrderCreated{
	Envelope: events.Envelope{EventID: "e-1", OccurredAt: "2026-01-01T00:00:00Z"},
	OrderID:  "o-1",
	RiderID:  "rider-1",
	Pickup:   events.LatLng{Lat: 10.776, Lng: 106.7},
	Dropoff:  events.LatLng{Lat: 10.79, Lng: 106.71},

	VehicleType: events.VehicleCar,
}

func TestRoundTripAndWireFormat(t *testing.T) {
	_, url := newRegistry(t)
	c := New(url)
	ctx := context.Background()

	data, err := c.Encode(ctx, events.TopicOrderCreated, sampleOrder)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0 {
		t.Errorf("magic byte = %d, want 0", data[0])
	}
	if id := binary.BigEndian.Uint32(data[1:5]); id != 1 {
		t.Errorf("schema id = %d, want 1", id)
	}

	var got events.OrderCreated
	if err := c.Decode(ctx, events.TopicOrderCreated, data, &got); err != nil {
		t.Fatal(err)
	}
	if got != sampleOrder {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", got, sampleOrder)
	}
}

func TestEveryTopicHasARoundTrippableSchema(t *testing.T) {
	_, url := newRegistry(t)
	c := New(url)
	ctx := context.Background()

	cases := map[string]any{
		events.TopicOrderCreated:         &events.OrderCreated{},
		events.TopicOrderMatched:         &events.OrderMatched{OrderID: "o", DriverID: "d"},
		events.TopicOrderNoDriverFound:   &events.OrderNoDriverFound{OrderID: "o"},
		events.TopicDriverLocationUpdate: &events.DriverLocationUpdated{DriverID: "d", Lat: 1, Lng: 2, Timestamp: "t"},
		events.TopicTripStatusChanged:    &events.TripStatusChanged{TripID: "t", OrderID: "o", DriverID: "d", Status: events.TripStatusCompleted},
		events.TopicTripRated:            &events.TripRated{TripID: "t", OrderID: "o", RiderID: "r", DriverID: "d", Stars: 5, Comment: "great"},
	}
	for topic, in := range cases {
		data, err := c.Encode(ctx, topic, in)
		if err != nil {
			t.Fatalf("%s: encode: %v", topic, err)
		}
		if err := c.Decode(ctx, topic, data, in); err != nil {
			t.Errorf("%s: decode: %v", topic, err)
		}
	}
}

// A producer still on the old schema, and a consumer already on a newer one
// with an added field that has a default: the consumer must still read the
// old messages and see the default.
func TestReadsOldMessagesAfterAddingAFieldWithADefault(t *testing.T) {
	_, url := newRegistry(t)
	ctx := context.Background()

	const v1 = `{"type":"record","name":"Ping","fields":[{"name":"id","type":"string"}]}`
	const v2 = `{"type":"record","name":"Ping","fields":[{"name":"id","type":"string"},{"name":"kind","type":"string","default":"CAR"}]}`

	source := func(text string) Option {
		return WithSchemaSource(func(string) (string, error) { return text, nil })
	}
	oldProducer := New(url, source(v1))
	newConsumer := New(url, source(v2))

	data, err := oldProducer.Encode(ctx, "ping", struct {
		ID string `avro:"id"`
	}{"p-1"})
	if err != nil {
		t.Fatal(err)
	}

	var got struct {
		ID   string `avro:"id"`
		Kind string `avro:"kind"`
	}
	if err := newConsumer.Decode(ctx, "ping", data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "p-1" || got.Kind != "CAR" {
		t.Errorf("got %+v, want id=p-1 kind=CAR (the default)", got)
	}
}

func TestRejectsMessagesWithoutTheMagicByte(t *testing.T) {
	_, url := newRegistry(t)
	c := New(url)

	var got events.OrderCreated
	err := c.Decode(context.Background(), events.TopicOrderCreated, []byte(`{"orderId":"o-1"}`), &got)
	if err == nil || !strings.Contains(err.Error(), "magic byte") {
		t.Errorf("err = %v, want a magic byte error", err)
	}
	var de *DecodeError
	if !errors.As(err, &de) || !de.Permanent() {
		t.Errorf("err = %T, want a permanent *DecodeError so it isn't retried", err)
	}
}

func TestSurfacesAnIncompatibleSchemaRejection(t *testing.T) {
	reg, url := newRegistry(t)
	reg.rejects = true

	_, err := New(url).Encode(context.Background(), events.TopicOrderCreated, sampleOrder)
	if err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Errorf("err = %v, want the registry's incompatibility message", err)
	}
}

func TestEncodeFailsWhenTheRegistryIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	if _, err := New(url).Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err == nil {
		t.Error("expected an error when the registry is down")
	}
}

func TestRegistersOncePerTopic(t *testing.T) {
	reg, url := newRegistry(t)
	c := New(url)
	for i := 0; i < 3; i++ {
		if _, err := c.Encode(context.Background(), events.TopicOrderCreated, sampleOrder); err != nil {
			t.Fatal(err)
		}
	}
	if len(reg.schemas) != 1 {
		t.Errorf("registered %d schemas, want 1", len(reg.schemas))
	}
}

// order.created as it was before vehicleType existed. Messages written with
// it are still sitting in Kafka after the upgrade, and must still be readable.
const orderCreatedV1 = `{"type":"record","name":"OrderCreated","namespace":"grabmini.events","fields":[
 {"name":"eventId","type":"string"},{"name":"occurredAt","type":"string"},{"name":"orderId","type":"string"},{"name":"riderId","type":"string"},
 {"name":"pickup","type":{"type":"record","name":"LatLng","fields":[{"name":"lat","type":"double"},{"name":"lng","type":"double"}]}},
 {"name":"dropoff","type":"LatLng"}]}`

func TestCurrentOrderCreatedReadsMessagesWrittenBeforeVehicleType(t *testing.T) {
	_, url := newRegistry(t)
	ctx := context.Background()

	oldProducer := New(url, WithSchemaSource(func(string) (string, error) { return orderCreatedV1, nil }))
	type v1Order struct {
		events.Envelope
		OrderID string        `avro:"orderId"`
		RiderID string        `avro:"riderId"`
		Pickup  events.LatLng `avro:"pickup"`
		Dropoff events.LatLng `avro:"dropoff"`
	}
	old := v1Order{sampleOrder.Envelope, "o-1", "rider-1", sampleOrder.Pickup, sampleOrder.Dropoff}
	data, err := oldProducer.Encode(ctx, events.TopicOrderCreated, old)
	if err != nil {
		t.Fatal(err)
	}

	var got events.OrderCreated
	if err := New(url).Decode(ctx, events.TopicOrderCreated, data, &got); err != nil {
		t.Fatal(err)
	}
	if got.OrderID != "o-1" || got.VehicleType != events.VehicleCar {
		t.Errorf("got orderId=%q vehicleType=%q, want o-1 and the CAR default", got.OrderID, got.VehicleType)
	}
}

func TestVehicleTypeSurvivesARoundTrip(t *testing.T) {
	_, url := newRegistry(t)
	c := New(url)
	in := sampleOrder
	in.VehicleType = events.VehicleBike

	data, err := c.Encode(context.Background(), events.TopicOrderCreated, in)
	if err != nil {
		t.Fatal(err)
	}
	var out events.OrderCreated
	if err := c.Decode(context.Background(), events.TopicOrderCreated, data, &out); err != nil {
		t.Fatal(err)
	}
	if out.VehicleType != events.VehicleBike {
		t.Errorf("vehicleType = %q, want BIKE", out.VehicleType)
	}
}
