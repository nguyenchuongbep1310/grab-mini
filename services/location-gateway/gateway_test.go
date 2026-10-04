package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
)

const testSecret = "0123456789abcdef0123456789abcdef"

type fakePub struct {
	sent []kafka.Message
	err  error
}

func (f *fakePub) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

// fakeEnc writes the event as "driver|lat|lng|timestamp", readable in assertions.
type fakeEnc struct{}

func (fakeEnc) Encode(_ context.Context, _ string, v any) ([]byte, error) {
	e := v.(events.DriverLocationUpdated)
	return []byte(strings.Join([]string{e.DriverID, f(e.Lat), f(e.Lng), e.Timestamp}, "|")), nil
}

func f(x float64) string { return strings.TrimRight(strings.TrimRight(strconvF(x), "0"), ".") }

type rig struct {
	g     *gateway
	pub   *fakePub
	mux   *http.ServeMux
	clock time.Time
	tok   func(role auth.Role, id string) string
}

func newRig(t *testing.T) *rig {
	t.Helper()
	issuer, _ := auth.NewIssuer(testSecret, time.Hour)
	verifier, _ := auth.NewVerifier(testSecret)
	r := &rig{pub: &fakePub{}, clock: time.Now().UTC().Truncate(time.Second)}
	r.g = &gateway{
		tracker: newTracker(time.Second, 5, 30*time.Second),
		pub:     r.pub, enc: fakeEnc{}, maxSkew: 30 * time.Second,
		now: func() time.Time { return r.clock },
	}
	r.mux = r.g.routes(verifier)
	r.tok = func(role auth.Role, id string) string { s, _, _ := issuer.Issue(role, id); return s }
	return r
}

func (r *rig) post(token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/locations", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	r.mux.ServeHTTP(rec, req)
	return rec
}

func TestAReportIsPublishedUnderTheTokensDriver(t *testing.T) {
	r := newRig(t)
	rec := r.post(r.tok(auth.Driver, "driver-7"), `{"lat":10.5,"lng":106.25}`)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"published":true`) {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	m := r.pub.sent[0]
	if string(m.Key) != "driver-7" || !strings.HasPrefix(string(m.Value), "driver-7|10.5|106.25|") {
		t.Errorf("published key=%s value=%s", m.Key, m.Value)
	}
}

func TestOnlyADriverWithAValidTokenMayReport(t *testing.T) {
	r := newRig(t)
	body := `{"lat":10,"lng":106}`
	if rec := r.post("", body); rec.Code != http.StatusUnauthorized {
		t.Errorf("no token: %d", rec.Code)
	}
	if rec := r.post(r.tok(auth.Rider, "rider-1"), body); rec.Code != http.StatusForbidden {
		t.Errorf("a rider: %d", rec.Code)
	}
	if len(r.pub.sent) != 0 {
		t.Error("something was published for a refused caller")
	}
}

func TestBadCoordinatesAreRefused(t *testing.T) {
	r := newRig(t)
	for name, body := range map[string]string{
		"not JSON":   `nope`,
		"no lat":     `{"lng":106}`,
		"no lng":     `{"lat":10}`,
		"lat > 90":   `{"lat":91,"lng":0}`,
		"lat < -90":  `{"lat":-90.5,"lng":0}`,
		"lng > 180":  `{"lat":0,"lng":181}`,
		"lat string": `{"lat":"10","lng":106}`,
		"huge body":  `{"lat":1,"lng":1,"timestamp":"` + strings.Repeat("a", 2000) + `"}`,
	} {
		if rec := r.post(r.tok(auth.Driver, "d"), body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", name, rec.Code)
		}
	}
	// 0,0 is a real place (and the zero value), so it must not be mistaken for "missing".
	if rec := r.post(r.tok(auth.Driver, "d"), `{"lat":0,"lng":0}`); rec.Code != http.StatusAccepted {
		t.Errorf("lat 0 lng 0: %d, want accepted", rec.Code)
	}
}

func TestADriverReportingTooOftenIsToldWhenToRetry(t *testing.T) {
	r := newRig(t)
	tok := r.tok(auth.Driver, "d")
	r.post(tok, `{"lat":10,"lng":106}`)

	r.clock = r.clock.Add(300 * time.Millisecond)
	rec := r.post(tok, `{"lat":10.01,"lng":106}`)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d Retry-After %q, want 429 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}

	r.clock = r.clock.Add(time.Second)
	if rec := r.post(tok, `{"lat":10.01,"lng":106}`); rec.Code != http.StatusAccepted {
		t.Errorf("after waiting: %d", rec.Code)
	}
	if len(r.pub.sent) != 2 {
		t.Errorf("%d published, want 2 (the refused one is not)", len(r.pub.sent))
	}
}

func TestTheLimitIsPerDriver(t *testing.T) {
	r := newRig(t)
	r.post(r.tok(auth.Driver, "a"), `{"lat":10,"lng":106}`)
	if rec := r.post(r.tok(auth.Driver, "b"), `{"lat":10,"lng":106}`); rec.Code != http.StatusAccepted {
		t.Errorf("another driver was limited: %d", rec.Code)
	}
}

func TestAParkedDriverIsNotRepublishedUntilTheHeartbeat(t *testing.T) {
	r := newRig(t)
	tok := r.tok(auth.Driver, "d")
	r.post(tok, `{"lat":10,"lng":106}`)

	r.clock = r.clock.Add(5 * time.Second)
	rec := r.post(tok, `{"lat":10.00001,"lng":106}`) // about a metre
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"published":false`) {
		t.Fatalf("a parked driver: %d %s, want accepted but not published", rec.Code, rec.Body)
	}
	if len(r.pub.sent) != 1 {
		t.Fatalf("%d published, want 1", len(r.pub.sent))
	}

	r.clock = r.clock.Add(31 * time.Second) // the heartbeat: the position must not go stale
	r.post(tok, `{"lat":10.00001,"lng":106}`)
	if len(r.pub.sent) != 2 {
		t.Errorf("%d published, want the heartbeat to publish", len(r.pub.sent))
	}
}

func TestAMovingDriverIsPublishedEveryTime(t *testing.T) {
	r := newRig(t)
	tok := r.tok(auth.Driver, "d")
	r.post(tok, `{"lat":10,"lng":106}`)
	r.clock = r.clock.Add(4 * time.Second)
	r.post(tok, `{"lat":10.001,"lng":106}`) // about 110 m
	if len(r.pub.sent) != 2 {
		t.Errorf("%d published, want both reports of a moving driver", len(r.pub.sent))
	}
}

func TestAFailedPublishDoesNotCostTheDriverTheirTurn(t *testing.T) {
	r := newRig(t)
	tok := r.tok(auth.Driver, "d")
	r.pub.err = errors.New("kafka down")
	if rec := r.post(tok, `{"lat":10,"lng":106}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}

	// Immediately again, with Kafka back: neither rate-limited nor taken for a duplicate.
	r.pub.err = nil
	if rec := r.post(tok, `{"lat":10,"lng":106}`); rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"published":true`) {
		t.Errorf("retry: %d %s", rec.Code, rec.Body)
	}
}

func TestTheDeviceClockIsTrustedOnlyWhenItAgreesWithOurs(t *testing.T) {
	r := newRig(t)
	now := r.clock
	cases := map[string]struct {
		device string
		want   time.Time
	}{
		"none":            {"", now},
		"close":           {now.Add(-10 * time.Second).Format(time.RFC3339), now.Add(-10 * time.Second)},
		"an hour ahead":   {now.Add(time.Hour).Format(time.RFC3339), now},
		"a day behind":    {now.Add(-24 * time.Hour).Format(time.RFC3339), now},
		"not a timestamp": {"yesterday", now},
		"in another zone": {now.In(time.FixedZone("x", 7*3600)).Format(time.RFC3339), now},
	}
	for name, c := range cases {
		if got := r.g.stamp(c.device, now); !got.Equal(c.want) {
			t.Errorf("%s: stamp = %s, want %s", name, got, c.want)
		}
	}
}

func TestIdleDriversAreForgotten(t *testing.T) {
	tr := newTracker(time.Second, 5, time.Minute)
	now := time.Now()
	tr.decide("old", 1, 1, now.Add(-time.Hour))
	tr.decide("recent", 1, 1, now)
	if n := tr.sweep(now, 10*time.Minute); n != 1 || tr.size() != 1 {
		t.Errorf("swept %d, %d left; want the idle one removed", n, tr.size())
	}
}

func TestDistance(t *testing.T) {
	if d := distanceMetres(10.776, 106.7, 10.776, 106.7); d != 0 {
		t.Errorf("same point: %f", d)
	}
	// One degree of latitude is about 111.2 km.
	if d := distanceMetres(10, 106, 11, 106); d < 111000 || d > 111400 {
		t.Errorf("one degree of latitude = %.0f m", d)
	}
}

// Runs the gateway's real producer settings (batching, lz4) against a broker.
func TestRealBrokerAcceptsTheGatewaysProducerSettings(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	topic := "gw-" + uuid.NewString()[:8]
	w := &kafka.Writer{
		Addr: kafka.TCP(brokers), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireOne,
		BatchTimeout: 20 * time.Millisecond, BatchSize: 500, Compression: kafka.Lz4, AllowAutoTopicCreation: true,
	}
	defer w.Close()

	var err error
	for i := 0; i < 5; i++ { // the first write may race topic creation
		if err = w.WriteMessages(context.Background(), kafka.Message{Key: []byte("d"), Value: []byte("hello")}); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// One key goes to one partition, but which one is up to the hash; try them all.
	for p := 0; p < 3; p++ {
		r2 := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokers}, Topic: topic, Partition: p, MaxWait: 200 * time.Millisecond})
		m, err := r2.ReadMessage(ctx)
		r2.Close()
		if err == nil {
			if string(m.Value) != "hello" {
				t.Errorf("read %q", m.Value)
			}
			return
		}
	}
	t.Fatal("the message never arrived")
}

func strconvF(x float64) string { return strconv.FormatFloat(x, 'f', -1, 64) }

func TestEveryReportOutcomeIsCounted(t *testing.T) {
	count := func(result string) float64 { return testutil.ToFloat64(locationReports.WithLabelValues(result)) }
	before := map[string]float64{}
	for _, r := range []string{"published", "stationary", "too_fast", "invalid", "error"} {
		before[r] = count(r)
	}

	r := newRig(t)
	tok := r.tok(auth.Driver, "metrics-driver")
	r.post(tok, `{"lat":10,"lng":106}`) // published
	r.clock = r.clock.Add(100 * time.Millisecond)
	r.post(tok, `{"lat":10,"lng":106}`) // too_fast
	r.clock = r.clock.Add(2 * time.Second)
	r.post(tok, `{"lat":10.000001,"lng":106}`)                 // stationary
	r.post(r.tok(auth.Driver, "other"), `{"lat":999,"lng":0}`) // invalid
	r.pub.err = errors.New("down")
	r.post(r.tok(auth.Driver, "third"), `{"lat":1,"lng":1}`) // error

	for result, want := range map[string]float64{"published": 1, "too_fast": 1, "stationary": 1, "invalid": 1, "error": 1} {
		if got := count(result) - before[result]; got != want {
			t.Errorf("%s moved by %v, want %v", result, got, want)
		}
	}
}
