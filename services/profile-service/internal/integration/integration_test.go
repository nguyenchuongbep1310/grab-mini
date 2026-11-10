package integration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	httpadapter "grab-mini/profile-service/internal/adapters/http"
	kafkaadapter "grab-mini/profile-service/internal/adapters/kafka"
	appmetrics "grab-mini/profile-service/internal/adapters/metrics"
	"grab-mini/profile-service/internal/adapters/postgres"
	"grab-mini/profile-service/internal/app"
)

// These tests need Postgres: set TEST_DATABASE_URL. Each test gets a database of its own
// (testdb_test.go), with the service's real migrations.

// outboxSource names this service in the outbox, as main does.
const outboxSource = "profile-service"

// jsonEncoder stands in for the Avro codec: the tests read back what was queued.
type jsonEncoder struct{}

func (jsonEncoder) Encode(_ context.Context, _ string, v any) ([]byte, error) { return json.Marshal(v) }

type failingEncoder struct{}

func (failingEncoder) Encode(context.Context, string, any) ([]byte, error) {
	return nil, fmt.Errorf("schema registry down")
}

// fixture is the service wired as main wires it, over the test's own database, with a
// clock the test moves (f.now), no relay and no public rate limit unless a test sets them.
// The use* methods change one dependency and rebuild the service and the routes.
type fixture struct {
	t        *testing.T
	db       *pgxpool.Pool
	issuer   *auth.Issuer
	verifier *auth.Verifier
	api      http.Handler
	now      time.Time

	enc     kafkaadapter.Codec
	relay   app.Waker // nil unless a test supplies a real relay (never a nil *outbox.Relay)
	limiter *auth.Limiter
	clock   func() time.Time // nil: the closure over f.now
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureOn(t, testDB(t))
}

// newFixtureOn builds the fixture over db, which may be a pool that is already closed.
func newFixtureOn(t *testing.T, db *pgxpool.Pool) *fixture {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	issuer, err := auth.NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{
		t: t, db: db, issuer: issuer, verifier: auth.NewVerifier(auth.NewKeySet(pub)),
		now: time.Now().UTC().Truncate(time.Second), enc: jsonEncoder{},
	}
	f.rebuild()
	return f
}

// rebuild wires app.Deps and the HTTP API again from the fixture's fields.
func (f *fixture) rebuild() {
	now := f.clock
	if now == nil {
		now = func() time.Time { return f.now }
	}
	store := postgres.NewStore(f.db)
	m := appmetrics.New()
	svc := app.New(app.Deps{
		Riders:  store,
		Drivers: store,
		Queue:   store,
		Resync:  store,
		Locker:  postgres.NewResyncLocker(f.db),
		UoW:     postgres.NewUnitOfWork(f.db, outboxSource),
		Encoder: kafkaadapter.NewEncoder(f.enc),
		Relay:   f.relay,
		Metrics: m,
		Now:     now,
	})
	f.api = httpadapter.NewAPI(svc, f.verifier, f.limiter, m).Routes()
}

func (f *fixture) useEncoder(c kafkaadapter.Codec) { f.enc = c; f.rebuild() }
func (f *fixture) useRelay(r app.Waker)            { f.relay = r; f.rebuild() }
func (f *fixture) useLimiter(l *auth.Limiter)      { f.limiter = l; f.rebuild() }
func (f *fixture) useClock(c func() time.Time)     { f.clock = c; f.rebuild() }

type result struct {
	status int
	header http.Header
	body   map[string]any
	raw    string
}

func (f *fixture) call(method, path string, role auth.Role, subject string, body any) result {
	f.t.Helper()
	var rd *bytes.Reader
	switch b := body.(type) {
	case nil:
		rd = bytes.NewReader(nil)
	case string:
		rd = bytes.NewReader([]byte(b))
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rd)
	if subject != "" {
		tok, _, err := f.issuer.Issue(role, subject)
		if err != nil {
			f.t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	f.api.ServeHTTP(rec, req)
	res := result{status: rec.Code, header: rec.Header(), body: map[string]any{}, raw: rec.Body.String()}
	json.Unmarshal(rec.Body.Bytes(), &res.body)
	return res
}

func wantStatus(t *testing.T, what string, r result, status int) {
	t.Helper()
	if r.status != status {
		t.Errorf("%s: status %d, want %d (%s)", what, r.status, status, r.raw)
	}
}

func wantCode(t *testing.T, what string, r result, status int, code string) {
	t.Helper()
	wantStatus(t, what, r, status)
	if r.body["code"] != code {
		t.Errorf("%s: code %v, want %q (%s)", what, r.body["code"], code, r.raw)
	}
}

// vehicle is a complete driver profile body.
func vehicle(plate string) map[string]any {
	return map[string]any{"name": "Nguyễn Văn An", "phone": "0901 234 567", "vehicleType": "CAR", "plate": plate, "model": "Toyota Vios", "color": "Trắng"}
}

func (f *fixture) putDriver(id string, body map[string]any) result {
	return f.call("PUT", "/drivers/"+id, auth.Driver, id, body)
}

func (f *fixture) apply(id string) result {
	return f.call("POST", "/drivers/"+id+"/application", auth.Driver, id, nil)
}

func (f *fixture) decide(action, id, reason string) result {
	var body any
	if reason != "" {
		body = map[string]string{"reason": reason}
	}
	return f.call("POST", "/drivers/"+id+"/"+action, auth.Admin, "ops-1", body)
}

// applied makes a PENDING driver; approved an APPROVED one.
func (f *fixture) applied(id string) {
	f.t.Helper()
	wantStatus(f.t, "put", f.putDriver(id, vehicle("51A-123.45")), 200)
	wantStatus(f.t, "apply", f.apply(id), 201)
}

func (f *fixture) approved(id string) {
	f.t.Helper()
	f.applied(id)
	wantStatus(f.t, "approve", f.decide("approve", id, ""), 200)
}

// queued returns the review events in the outbox, oldest first.
func (f *fixture) queued() []events.DriverApplicationReviewed {
	f.t.Helper()
	rows, err := f.db.Query(context.Background(), `SELECT topic, key, payload FROM outbox WHERE source = 'profile-service' ORDER BY id`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []events.DriverApplicationReviewed
	for rows.Next() {
		var topic string
		var key, payload []byte
		if err := rows.Scan(&topic, &key, &payload); err != nil {
			f.t.Fatal(err)
		}
		if topic != events.TopicDriverApplicationReviewed {
			f.t.Fatalf("event on topic %q", topic)
		}
		var evt events.DriverApplicationReviewed
		if err := json.Unmarshal(payload, &evt); err != nil {
			f.t.Fatal(err)
		}
		if string(key) != evt.DriverID {
			f.t.Errorf("key %q, driver %q: the key must be the driver", key, evt.DriverID)
		}
		out = append(out, evt)
	}
	return out
}

func (f *fixture) statusOf(id string) string {
	f.t.Helper()
	var s string
	if err := f.db.QueryRow(context.Background(), `SELECT status FROM driver_profiles WHERE driver_id = $1`, id).Scan(&s); err != nil {
		f.t.Fatal(err)
	}
	return s
}

// ---------------------------------------------------------------- riders ----

func TestARiderReadsAndWritesOnlyTheirOwnProfile(t *testing.T) {
	f := newFixture(t)

	r := f.call("GET", "/riders/rider-1", auth.Rider, "rider-1", nil)
	wantStatus(t, "first visit", r, 200)
	if r.body["name"] != "" || r.body["updatedAt"] != nil {
		t.Errorf("a rider with no profile should get an empty one: %s", r.raw)
	}
	if r.header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q: personal data must not be cached", r.header.Get("Cache-Control"))
	}

	r = f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "  Hoàng Gia Bảo ", "phone": "+84 901 234 567"})
	wantStatus(t, "put", r, 200)
	if r.body["name"] != "Hoàng Gia Bảo" || r.body["phone"] != "+84 901 234 567" || r.body["riderId"] != "rider-1" {
		t.Errorf("put returned %s", r.raw)
	}
	r = f.call("GET", "/riders/rider-1", auth.Rider, "rider-1", nil)
	if r.body["name"] != "Hoàng Gia Bảo" || r.body["updatedAt"] == nil {
		t.Errorf("get after put: %s", r.raw)
	}
	// Replaces, and the phone can be cleared.
	f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Bảo"})
	r = f.call("GET", "/riders/rider-1", auth.Rider, "rider-1", nil)
	if r.body["name"] != "Bảo" || r.body["phone"] != "" {
		t.Errorf("after replacing: %s", r.raw)
	}
	if n := len(f.queued()); n != 0 {
		t.Errorf("a rider profile published %d event(s)", n)
	}
}

func TestProfilesBelongToTheCaller(t *testing.T) {
	f := newFixture(t)
	f.putDriver("driver-1", vehicle("51A-123.45"))
	f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Bảo"})

	// Someone else's id, whether or not it exists: 403, so the answer says nothing about who has a profile.
	for _, id := range []string{"rider-1", "rider-2", "nobody"} {
		wantCode(t, "rider reads "+id, f.call("GET", "/riders/"+id, auth.Rider, "rider-9", nil), 403, "forbidden")
		wantCode(t, "rider writes "+id, f.call("PUT", "/riders/"+id, auth.Rider, "rider-9", map[string]string{"name": "X"}), 403, "forbidden")
	}
	for _, id := range []string{"driver-1", "driver-2", "nobody"} {
		wantCode(t, "driver reads "+id, f.call("GET", "/drivers/"+id, auth.Driver, "driver-9", nil), 403, "forbidden")
		wantCode(t, "driver writes "+id, f.call("PUT", "/drivers/"+id, auth.Driver, "driver-9", vehicle("51A-123.45")), 403, "forbidden")
		wantCode(t, "driver applies "+id, f.call("POST", "/drivers/"+id+"/application", auth.Driver, "driver-9", nil), 403, "forbidden")
	}
	// The other role, and an admin, have no way in either.
	wantStatus(t, "driver token on a rider profile", f.call("GET", "/riders/rider-1", auth.Driver, "rider-1", nil), 403)
	wantStatus(t, "rider token on a driver profile", f.call("GET", "/drivers/driver-1", auth.Rider, "driver-1", nil), 403)
	wantStatus(t, "admin reads a rider", f.call("GET", "/riders/rider-1", auth.Admin, "rider-1", nil), 403)
	wantStatus(t, "admin reads a driver", f.call("GET", "/drivers/driver-1", auth.Admin, "driver-1", nil), 403)
	wantStatus(t, "no token", f.call("GET", "/riders/rider-1", "", "", nil), 401)
	wantStatus(t, "no token (driver)", f.call("PUT", "/drivers/driver-1", "", "", vehicle("51A-123.45")), 401)
	// The admin routes are for admins only.
	for _, role := range []auth.Role{auth.Rider, auth.Driver} {
		wantStatus(t, "list as "+string(role), f.call("GET", "/driver-applications", role, "x", nil), 403)
		wantStatus(t, "approve as "+string(role), f.call("POST", "/drivers/driver-1/approve", role, "driver-1", nil), 403)
	}
	// The body cannot name another person or set the status.
	r := f.call("PUT", "/drivers/driver-1", auth.Driver, "driver-1", `{"name":"An","vehicleType":"CAR","plate":"51A-123.45","driverId":"driver-2","status":"APPROVED","reason":"x","reviewedBy":"me"}`)
	wantStatus(t, "extra fields are ignored", r, 200)
	if r.body["driverId"] != "driver-1" || r.body["status"] != "NONE" {
		t.Errorf("the body set identity or status: %s", r.raw)
	}
}

func TestRiderAndDriverValidation(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct {
		what  string
		body  any
		field string
	}{
		{"no name", map[string]string{"phone": "0901234567"}, "name"},
		{"blank name", map[string]string{"name": "   "}, "name"},
		{"name with a newline", map[string]string{"name": "A\nB"}, "name"},
		{"long name", map[string]string{"name": strings.Repeat("a", 81)}, "name"},
		{"bad phone", map[string]string{"name": "A", "phone": "call me"}, "phone"},
		{"short phone", map[string]string{"name": "A", "phone": "12345"}, "phone"},
	} {
		r := f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", c.body)
		wantCode(t, "rider: "+c.what, r, 400, "invalid_profile")
		if r.body["field"] != c.field {
			t.Errorf("rider: %s: field %v, want %s", c.what, r.body["field"], c.field)
		}
	}
	for _, body := range []string{"", "{", "[]", `"x"`, `{"name":5}`} {
		wantCode(t, "rider body "+body, f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", body), 400, "invalid_request")
	}
	if n := f.count(`SELECT count(*) FROM rider_profiles`); n != 0 {
		t.Errorf("%d rider rows after only invalid requests", n)
	}

	v := func(k string, val any) map[string]any {
		b := vehicle("51A-123.45")
		b[k] = val
		return b
	}
	for _, c := range []struct {
		what, field string
		body        map[string]any
	}{
		{"truck", "vehicleType", v("vehicleType", "TRUCK")},
		{"plate with symbols", "plate", v("plate", "51A#123")},
		{"short plate", "plate", v("plate", "A1")},
		{"long model", "model", v("model", strings.Repeat("m", 51))},
		{"long colour", "color", v("color", strings.Repeat("c", 31))},
		{"colour with a control character", "color", v("color", "red\x07")},
		{"no name", "name", v("name", "")},
	} {
		r := f.putDriver("driver-1", c.body)
		wantCode(t, "driver: "+c.what, r, 400, "invalid_profile")
		if r.body["field"] != c.field {
			t.Errorf("driver: %s: field %v, want %s", c.what, r.body["field"], c.field)
		}
	}
	if n := f.count(`SELECT count(*) FROM driver_profiles`); n != 0 {
		t.Errorf("%d driver rows after only invalid requests", n)
	}
	// A draft may leave the vehicle empty until the driver applies.
	r := f.putDriver("driver-1", map[string]any{"name": "An"})
	wantStatus(t, "draft", r, 200)
	if r.body["status"] != "NONE" {
		t.Errorf("status %v", r.body["status"])
	}
}

func (f *fixture) count(sql string) int {
	f.t.Helper()
	var n int
	if err := f.db.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// ------------------------------------------------------------ onboarding ----

func TestADriverApplies(t *testing.T) {
	f := newFixture(t)

	r := f.call("GET", "/drivers/driver-1", auth.Driver, "driver-1", nil)
	wantStatus(t, "first visit", r, 200)
	if r.body["status"] != "NONE" || r.body["driverId"] != "driver-1" {
		t.Errorf("a driver with no profile: %s", r.raw)
	}

	// Nothing saved yet, then a draft without a vehicle: not enough to apply.
	wantCode(t, "no profile", f.apply("driver-1"), 422, "incomplete_profile")
	wantStatus(t, "draft", f.putDriver("driver-1", map[string]any{"name": "An"}), 200)
	r = f.apply("driver-1")
	wantCode(t, "draft", r, 422, "incomplete_profile")
	if !strings.Contains(r.raw, "vehicleType") || !strings.Contains(r.raw, "plate") {
		t.Errorf("the refusal should say what is missing: %s", r.raw)
	}

	wantStatus(t, "put", f.putDriver("driver-1", vehicle("51a-123.45")), 200)
	r = f.apply("driver-1")
	wantStatus(t, "apply", r, 201)
	if r.body["status"] != "PENDING" || r.body["plate"] != "51A-123.45" {
		t.Errorf("after applying: %s", r.raw)
	}
	// Retrying is safe and changes nothing.
	statusAt := r.body["statusAt"]
	f.now = f.now.Add(time.Minute)
	r = f.apply("driver-1")
	wantStatus(t, "apply again", r, 200)
	if r.body["status"] != "PENDING" || r.body["statusAt"] != statusAt {
		t.Errorf("a repeat changed the application: %s", r.raw)
	}
	if n := len(f.queued()); n != 0 {
		t.Errorf("applying published %d event(s): PENDING is not a review result", n)
	}
	r = f.call("GET", "/drivers/driver-1", auth.Driver, "driver-1", nil)
	if r.body["status"] != "PENDING" || r.body["name"] != "Nguyễn Văn An" || r.body["phone"] != "0901 234 567" {
		t.Errorf("get: %s", r.raw)
	}
	if _, has := r.body["reviewedBy"]; has {
		t.Error("a driver is not shown which admin reviewed them")
	}
}

func TestReviewTransitions(t *testing.T) {
	f := newFixture(t)
	type step struct {
		action string
		reason string
		want   int
		status string // the driver's status afterwards
	}
	// Each driver walks one path; 409 leaves the status where it was.
	paths := map[string][]step{
		"approve": {{"approve", "", 200, "APPROVED"}, {"approve", "", 409, "APPROVED"}, {"reject", "no", 409, "APPROVED"}},
		"reject":  {{"reject", "documents unreadable", 200, "REJECTED"}, {"approve", "", 409, "REJECTED"}, {"suspend", "x", 409, "REJECTED"}, {"reject", "again", 409, "REJECTED"}},
		"suspend": {{"approve", "", 200, "APPROVED"}, {"suspend", "complaints", 200, "SUSPENDED"}, {"suspend", "again", 409, "SUSPENDED"}, {"reject", "x", 409, "SUSPENDED"}, {"approve", "reinstated", 200, "APPROVED"}},
		"pending": {{"suspend", "x", 409, "PENDING"}},
	}
	for name, steps := range paths {
		id := "driver-" + name
		f.applied(id)
		for i, s := range steps {
			r := f.decide(s.action, id, s.reason)
			wantStatus(t, fmt.Sprintf("%s step %d (%s)", name, i, s.action), r, s.want)
			if s.want == 409 {
				wantCode(t, name, r, 409, "invalid_transition")
				if r.body["status"] != f.statusOf(id) {
					t.Errorf("%s step %d: the 409 says %v, the driver is %s", name, i, r.body["status"], f.statusOf(id))
				}
			}
			if got := f.statusOf(id); got != s.status {
				t.Errorf("%s step %d (%s): status %s, want %s", name, i, s.action, got, s.status)
			}
		}
	}
}

func TestEveryDecisionIsOneEventInTheSameTransaction(t *testing.T) {
	f := newFixture(t)
	f.applied("driver-1")
	if n := len(f.queued()); n != 0 {
		t.Fatalf("%d events before any decision", n)
	}

	r := f.decide("approve", "driver-1", "")
	wantStatus(t, "approve", r, 200)
	if r.body["reviewedBy"] != "ops-1" || r.body["status"] != "APPROVED" {
		t.Errorf("approve returned %s", r.raw)
	}
	f.now = f.now.Add(time.Minute)
	wantStatus(t, "suspend", f.decide("suspend", "driver-1", "Complaint 4412: unsafe driving"), 200)
	f.now = f.now.Add(time.Minute)
	wantStatus(t, "approve again", f.decide("approve", "driver-1", "Reinstated after review"), 200)

	got := f.queued()
	if len(got) != 3 {
		t.Fatalf("%d events, want 3: %+v", len(got), got)
	}
	want := []struct{ status, reason string }{{"APPROVED", ""}, {"SUSPENDED", "Complaint 4412: unsafe driving"}, {"APPROVED", "Reinstated after review"}}
	var last time.Time
	for i, evt := range got {
		if evt.Status != want[i].status || evt.Reason != want[i].reason || evt.DriverID != "driver-1" ||
			evt.VehicleType != events.VehicleCar || evt.ReviewedBy != "ops-1" {
			t.Errorf("event %d = %+v", i, evt)
		}
		if _, err := uuid.Parse(evt.EventID); err != nil {
			t.Errorf("event %d: eventId %q is not a UUID", i, evt.EventID)
		}
		at, err := time.Parse(time.RFC3339Nano, evt.OccurredAt)
		if err != nil || !at.After(last) {
			t.Errorf("event %d: occurredAt %q is not after the previous (%v), %v", i, evt.OccurredAt, last, err)
		}
		last = at
	}

	// A refused decision queues nothing.
	f.decide("approve", "driver-1", "")
	f.decide("reject", "driver-1", "x")
	if n := len(f.queued()); n != 3 {
		t.Errorf("%d events after refused decisions, want 3", n)
	}
}

func TestTheEventIsNotPublishedWhenItCannotBeQueued(t *testing.T) {
	f := newFixture(t)
	f.applied("driver-1")
	f.useEncoder(failingEncoder{})
	wantStatus(t, "approve with the registry down", f.decide("approve", "driver-1", ""), 500)
	if got := f.statusOf("driver-1"); got != "PENDING" {
		t.Errorf("status %s: a decision whose event could not be queued must not be saved", got)
	}
	f.useEncoder(jsonEncoder{})
	wantStatus(t, "approve again", f.decide("approve", "driver-1", ""), 200)
	if n := len(f.queued()); n != 1 {
		t.Errorf("%d events, want 1", n)
	}
}

func TestResultTimesIncreaseEvenIfTheClockStepsBack(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	f.decide("suspend", "driver-1", "x")
	f.now = f.now.Add(-time.Hour) // a clock that went backwards, or a second instance that is behind
	wantStatus(t, "approve", f.decide("approve", "driver-1", ""), 200)
	got := f.queued()
	if len(got) != 3 {
		t.Fatalf("%d events", len(got))
	}
	for i := 1; i < len(got); i++ {
		a, _ := time.Parse(time.RFC3339Nano, got[i-1].OccurredAt)
		b, _ := time.Parse(time.RFC3339Nano, got[i].OccurredAt)
		if !b.After(a) {
			t.Errorf("event %d (%s) is not after event %d (%s)", i, got[i].OccurredAt, i-1, got[i-1].OccurredAt)
		}
	}
}

func TestReasonsAreRequiredToRejectAndSuspend(t *testing.T) {
	f := newFixture(t)
	f.applied("driver-1")
	wantCode(t, "reject with no reason", f.decide("reject", "driver-1", ""), 400, "invalid_profile")
	wantCode(t, "reject with a blank reason", f.call("POST", "/drivers/driver-1/reject", auth.Admin, "ops-1", `{"reason":"   "}`), 400, "invalid_profile")
	wantCode(t, "reject with a long reason", f.decide("reject", "driver-1", strings.Repeat("x", 501)), 400, "invalid_profile")
	wantCode(t, "reject with a newline", f.decide("reject", "driver-1", "a\nb"), 400, "invalid_profile")
	wantCode(t, "bad body", f.call("POST", "/drivers/driver-1/reject", auth.Admin, "ops-1", "{"), 400, "invalid_request")
	if got := f.statusOf("driver-1"); got != "PENDING" {
		t.Fatalf("status %s after refused rejections", got)
	}
	f.approved("driver-2")
	wantCode(t, "suspend with no reason", f.decide("suspend", "driver-2", ""), 400, "invalid_profile")
	// Approving needs none, and accepts an empty body or none.
	wantStatus(t, "approve with an empty object", f.call("POST", "/drivers/driver-1/approve", auth.Admin, "ops-1", "{}"), 200)

	r := f.decide("reject", "nobody", "x")
	wantCode(t, "unknown driver", r, 404, "not_found")
	wantCode(t, "unknown driver (approve)", f.decide("approve", "nobody", ""), 404, "not_found")
	// The driver sees the reason of a rejection.
	f.applied("driver-3")
	f.decide("reject", "driver-3", "Photo of the plate is unreadable")
	r = f.call("GET", "/drivers/driver-3", auth.Driver, "driver-3", nil)
	if r.body["status"] != "REJECTED" || r.body["reason"] != "Photo of the plate is unreadable" {
		t.Errorf("rejected driver sees %s", r.raw)
	}
	// ...and can apply again; the old reason goes.
	r = f.apply("driver-3")
	wantStatus(t, "apply after rejection", r, 201)
	if r.body["status"] != "PENDING" || r.body["reason"] != "" {
		t.Errorf("new application: %s", r.raw)
	}
}

func TestASuspendedDriverCannotApplyTheirWayBack(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	wantStatus(t, "suspend", f.decide("suspend", "driver-1", "x"), 200)
	wantCode(t, "apply", f.apply("driver-1"), 409, "suspended")
	// Nor does editing: even a new plate keeps them suspended, and publishes nothing more.
	before := len(f.queued())
	r := f.putDriver("driver-1", vehicle("29A-999.99"))
	wantStatus(t, "edit", r, 200)
	if r.body["status"] != "SUSPENDED" {
		t.Errorf("an edit changed a suspension: %s", r.raw)
	}
	if n := len(f.queued()); n != before {
		t.Errorf("an edit of a suspended driver published %d event(s)", n-before)
	}
}

// ------------------------------------------------- vehicle changes (safety) ----

func TestChangingThePlateOrVehicleOfAnApprovedDriverTakesThemOutOfService(t *testing.T) {
	for _, change := range []struct {
		what string
		edit func(map[string]any)
	}{
		{"plate", func(b map[string]any) { b["plate"] = "29A-999.99" }},
		{"vehicle type", func(b map[string]any) { b["vehicleType"] = "BIKE" }},
		{"plate only in case and spacing is the same plate", nil},
	} {
		t.Run(change.what, func(t *testing.T) {
			f := newFixture(t)
			f.approved("driver-1")
			base := len(f.queued())

			body := vehicle("51a-123.45") // same plate, as typed differently
			if change.edit != nil {
				change.edit(body)
			}
			r := f.putDriver("driver-1", body)
			wantStatus(t, "edit", r, 200)

			if change.edit == nil {
				if r.body["status"] != "APPROVED" || len(f.queued()) != base {
					t.Errorf("an unchanged vehicle took the driver out of service: %s, %d new events", r.raw, len(f.queued())-base)
				}
				return
			}
			if r.body["status"] != "PENDING" || !strings.Contains(fmt.Sprint(r.body["reason"]), "approve") {
				t.Errorf("the driver should be PENDING with a reason: %s", r.raw)
			}
			got := f.queued()
			if len(got) != base+1 {
				t.Fatalf("%d events, want %d", len(got), base+1)
			}
			evt := got[len(got)-1]
			if evt.Status != "SUSPENDED" || evt.ReviewedBy != "system" || evt.DriverID != "driver-1" || evt.Reason == "" {
				t.Errorf("event = %+v, want a SUSPENDED result from the system", evt)
			}
			if evt.VehicleType != events.VehicleType(body["vehicleType"].(string)) {
				t.Errorf("event vehicle %s", evt.VehicleType)
			}

			// Repeating the same edit changes nothing more (a retry).
			wantStatus(t, "same edit again", f.putDriver("driver-1", body), 200)
			if n := len(f.queued()); n != base+1 {
				t.Errorf("a retry published another event (%d)", n)
			}
			// The public view is gone until an admin approves the new vehicle...
			wantStatus(t, "public view while pending", f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil), 404)
			// ...and approving brings it back with the new plate and a later time.
			f.now = f.now.Add(time.Minute)
			wantStatus(t, "approve", f.decide("approve", "driver-1", ""), 200)
			got = f.queued()
			last := got[len(got)-1]
			if last.Status != "APPROVED" || last.VehicleType != evt.VehicleType {
				t.Errorf("approval event %+v", last)
			}
			a, _ := time.Parse(time.RFC3339Nano, evt.OccurredAt)
			b, _ := time.Parse(time.RFC3339Nano, last.OccurredAt)
			if !b.After(a) {
				t.Errorf("approval %s is not after the vehicle change %s", last.OccurredAt, evt.OccurredAt)
			}
		})
	}
}

func TestDescribingTheVehicleDoesNotNeedAReview(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	base := len(f.queued())
	body := vehicle("51A-123.45")
	body["model"], body["color"], body["phone"] = "Mazda 3", "Xám", "0988 000 111"
	r := f.putDriver("driver-1", body)
	wantStatus(t, "edit", r, 200)
	if r.body["status"] != "APPROVED" || r.body["model"] != "Mazda 3" || len(f.queued()) != base {
		t.Errorf("model and colour are descriptions: %s, %d new events", r.raw, len(f.queued())-base)
	}
}

func TestAProfileUnderReviewCannotLoseItsVehicle(t *testing.T) {
	f := newFixture(t)
	f.applied("driver-1")
	b := vehicle("")
	r := f.putDriver("driver-1", b)
	wantCode(t, "blank plate while pending", r, 400, "invalid_profile")
	b = vehicle("51A-123.45")
	b["vehicleType"] = ""
	wantCode(t, "blank vehicle while pending", f.putDriver("driver-1", b), 400, "invalid_profile")
	if got := f.statusOf("driver-1"); got != "PENDING" {
		t.Errorf("status %s", got)
	}
	// A pending driver may change the plate (the application is still waiting): still PENDING, no event.
	r = f.putDriver("driver-1", vehicle("29A-999.99"))
	wantStatus(t, "edit while pending", r, 200)
	if r.body["status"] != "PENDING" || len(f.queued()) != 0 {
		t.Errorf("%s, %d events", r.raw, len(f.queued()))
	}
	// A rejected driver edits to fix the application, and is still rejected until they apply.
	f.decide("reject", "driver-1", "wrong plate")
	r = f.putDriver("driver-1", vehicle("29A-888.88"))
	if r.body["status"] != "REJECTED" || r.body["reason"] != "wrong plate" {
		t.Errorf("an edit changed a rejection: %s", r.raw)
	}
}

// ---------------------------------------------------------------- concurrency ----

func TestConcurrentDecisionsOnOneDriverHaveOneWinner(t *testing.T) {
	f := newFixture(t)
	f.applied("driver-1")

	var wg sync.WaitGroup
	codes := make([]int, 12)
	start := make(chan struct{})
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				codes[i] = f.decide("approve", "driver-1", "").status
			} else {
				codes[i] = f.decide("reject", "driver-1", "no").status
			}
		}()
	}
	close(start)
	wg.Wait()

	ok := 0
	for _, c := range codes {
		if c == 200 {
			ok++
		} else if c != 409 {
			t.Errorf("status %d", c)
		}
	}
	if ok != 1 {
		t.Errorf("%d decisions succeeded, want exactly 1 (%v)", ok, codes)
	}
	evts := f.queued()
	if len(evts) != 1 || evts[0].Status != f.statusOf("driver-1") {
		t.Errorf("events %+v for a driver who is %s", evts, f.statusOf("driver-1"))
	}
}

func TestConcurrentVehicleChangesAndDecisionsKeepTheEventsInOrder(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.putDriver("driver-1", vehicle(fmt.Sprintf("51A-%03d.00", i)))
			f.decide("approve", "driver-1", "")
			f.decide("suspend", "driver-1", "x")
		}()
	}
	wg.Wait()
	// Whatever the interleaving, the times (and so the order a consumer applies them in) strictly increase
	// in the order the events were queued, and the last one agrees with the final state.
	evts := f.queued()
	var last time.Time
	for i, evt := range evts {
		at, err := time.Parse(time.RFC3339Nano, evt.OccurredAt)
		if err != nil || !at.After(last) {
			t.Fatalf("event %d (%s) is not after the previous (%v)", i, evt.OccurredAt, last)
		}
		last = at
	}
	final := evts[len(evts)-1].Status
	switch f.statusOf("driver-1") {
	case "APPROVED":
		if final != "APPROVED" {
			t.Errorf("driver APPROVED, last event %s", final)
		}
	case "SUSPENDED", "PENDING":
		if final != "SUSPENDED" {
			t.Errorf("driver out of service, last event %s", final)
		}
	}
}

// ---------------------------------------------------------------- public view ----

func TestPublicViewShowsOnlyAnApprovedDriversVehicle(t *testing.T) {
	f := newFixture(t)
	get := func(id string) result { return f.call("GET", "/drivers/"+id+"/public", auth.Rider, "rider-1", nil) }

	// Unknown, draft, pending, rejected, suspended: all the same 404.
	f.putDriver("draft", vehicle("51A-100.00"))
	f.applied("pending")
	f.applied("rejected")
	f.decide("reject", "rejected", "x")
	f.approved("suspended")
	f.decide("suspend", "suspended", "x")
	for _, id := range []string{"nobody", "draft", "pending", "rejected", "suspended"} {
		r := get(id)
		wantCode(t, id, r, 404, "not_found")
		if strings.Contains(r.raw, "Nguyễn") || strings.Contains(r.raw, "51A") {
			t.Errorf("%s: the refusal leaked details: %s", id, r.raw)
		}
	}

	f.approved("driver-1")
	for _, c := range []struct {
		role auth.Role
		sub  string
	}{{auth.Rider, "rider-1"}, {auth.Driver, "driver-7"}, {auth.Admin, "ops-1"}, {auth.Driver, "driver-1"}} {
		r := f.call("GET", "/drivers/driver-1/public", c.role, c.sub, nil)
		wantStatus(t, string(c.role), r, 200)
		keys := map[string]bool{}
		for k := range r.body {
			keys[k] = true
		}
		want := map[string]bool{"driverId": true, "name": true, "vehicleType": true, "plate": true, "model": true, "color": true}
		for k := range keys {
			if !want[k] {
				t.Errorf("the public view has an extra field %q: %s", k, r.raw)
			}
		}
		for k := range want {
			if !keys[k] {
				t.Errorf("the public view lacks %q: %s", k, r.raw)
			}
		}
		if strings.Contains(r.raw, "0901") || strings.Contains(r.raw, "phone") || strings.Contains(r.raw, "status") {
			t.Errorf("the public view leaked the phone or the status: %s", r.raw)
		}
		if r.body["name"] != "Nguyễn Văn An" || r.body["plate"] != "51A-123.45" || r.body["vehicleType"] != "CAR" || r.body["color"] != "Trắng" {
			t.Errorf("public view: %s", r.raw)
		}
	}
	wantStatus(t, "no token", f.call("GET", "/drivers/driver-1/public", "", "", nil), 401)
}

func TestPublicLookupsAreRateLimitedPerCaller(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	f.useLimiter(auth.NewLimiter(1.0/60, 3)) // a burst of 3, then one a minute
	got := map[int]int{}
	for i := 0; i < 6; i++ {
		r := f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil)
		got[r.status]++
		if r.status == 429 {
			if r.header.Get("Retry-After") == "" || r.body["retryAfter"] == nil || r.body["code"] != "rate_limited" {
				t.Errorf("429 without Retry-After: %s %v", r.raw, r.header)
			}
		}
	}
	if got[200] != 3 || got[429] != 3 {
		t.Errorf("statuses %v, want 3 x 200 and 3 x 429", got)
	}
	// Another caller has their own allowance.
	wantStatus(t, "another caller", f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-2", nil), 200)
}

// ---------------------------------------------------------------- admin queue ----

func TestTheAdminQueueIsOldestFirstAndPaged(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("driver-%d", i)
		f.putDriver(id, vehicle(fmt.Sprintf("51A-%03d.00", i)))
		f.now = f.now.Add(time.Minute)
		f.apply(id)
	}
	f.now = f.now.Add(time.Minute)
	// driver-6 applied at the same moment as driver-5: the id breaks the tie.
	f.putDriver("driver-6", vehicle("51A-006.00"))
	f.now = f.now.Add(-time.Minute)
	f.apply("driver-6")
	f.putDriver("driver-0", vehicle("51A-000.00")) // a draft: not in the queue

	var ids []string
	cursor := ""
	pages := 0
	for {
		path := "/driver-applications?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := f.call("GET", path, auth.Admin, "ops-1", nil)
		wantStatus(t, "page", r, 200)
		apps := r.body["applications"].([]any)
		for _, a := range apps {
			m := a.(map[string]any)
			ids = append(ids, m["driverId"].(string))
			if m["status"] != "PENDING" || m["phone"] == "" || m["plate"] == "" {
				t.Errorf("an admin should see the whole application: %v", m)
			}
		}
		pages++
		if r.body["nextCursor"] == nil {
			break
		}
		cursor = r.body["nextCursor"].(string)
		if pages > 10 {
			t.Fatal("paging does not end")
		}
	}
	if strings.Join(ids, ",") != "driver-1,driver-2,driver-3,driver-4,driver-5,driver-6" && strings.Join(ids, ",") != "driver-1,driver-2,driver-3,driver-4,driver-6,driver-5" {
		t.Errorf("order: %v", ids)
	}
	if pages != 3 {
		t.Errorf("%d pages of 2 for 6 applications", pages)
	}

	// A decision takes a driver out of the queue; the default status is PENDING.
	f.decide("approve", "driver-1", "")
	r := f.call("GET", "/driver-applications", auth.Admin, "ops-1", nil)
	if n := len(r.body["applications"].([]any)); n != 5 {
		t.Errorf("%d pending after one approval, want 5", n)
	}
	r = f.call("GET", "/driver-applications?status=APPROVED", auth.Admin, "ops-1", nil)
	apps := r.body["applications"].([]any)
	if len(apps) != 1 || apps[0].(map[string]any)["reviewedBy"] != "ops-1" {
		t.Errorf("approved list: %s", r.raw)
	}
	for _, q := range []string{"status=NONE", "status=pending", "status=ALL", "limit=0", "limit=101", "limit=x", "cursor=junk", "cursor=-1.x", "cursor=5."} {
		wantCode(t, q, f.call("GET", "/driver-applications?"+q, auth.Admin, "ops-1", nil), 400, "invalid_request")
	}
	// An empty queue is an empty list, not null.
	r = f.call("GET", "/driver-applications?status=SUSPENDED", auth.Admin, "ops-1", nil)
	if !strings.Contains(r.raw, `"applications":[]`) {
		t.Errorf("empty queue: %s", r.raw)
	}
}

// ---------------------------------------------------------------- privacy ----

// Names, phones and plates are personal data: the logs carry ids only.
func TestPersonalDataIsNeverLogged(t *testing.T) {
	f := newFixture(t)
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	secrets := []string{"Zephyrine Quillfeather", "0912 345 678", "77Z-987.65", "Batmobile", "Chartreuse", "0977 111 222", "Wilhelmina Fogbottom", "Plaid Unicorn"}
	f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": secrets[6], "phone": secrets[5]})
	f.call("GET", "/riders/rider-1", auth.Rider, "rider-1", nil)
	f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": secrets[6], "phone": "not a phone"}) // refused
	body := map[string]any{"name": secrets[0], "phone": secrets[1], "vehicleType": "CAR", "plate": secrets[2], "model": secrets[3], "color": secrets[4]}
	f.putDriver("driver-1", body)
	f.apply("driver-1")
	f.decide("approve", "driver-1", "ok")
	f.call("GET", "/drivers/driver-1", auth.Driver, "driver-1", nil)
	f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil)
	body["plate"] = "88B-111.22" // a vehicle change: logged by id
	f.putDriver("driver-1", body)
	f.decide("approve", "driver-1", "")
	f.decide("suspend", "driver-1", "reason text is fine")
	f.call("GET", "/driver-applications?status=SUSPENDED", auth.Admin, "ops-1", nil)
	f.putDriver("driver-2", map[string]any{"name": secrets[7], "plate": "bad#plate"}) // refused
	f.useEncoder(failingEncoder{})
	f.putDriver("driver-3", body)
	f.apply("driver-3")
	f.decide("approve", "driver-3", "")
	f.useEncoder(jsonEncoder{})

	logged := buf.String()
	if strings.TrimSpace(logged) == "" {
		t.Fatal("nothing was logged at all: the test checks nothing")
	}
	for _, s := range append(secrets, "88B-111.22", "not a phone", "bad#plate") {
		if strings.Contains(logged, s) {
			t.Errorf("%q reached the log:\n%s", s, logged)
		}
	}
	if !strings.Contains(logged, "driver-1") {
		t.Errorf("expected ids in the log, got:\n%s", logged)
	}
}

// ---------------------------------------------------------------- seed ----

func TestSeedDevAddsApprovedProfilesOnceAndPublishesNothing(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	d, r, err := postgres.SeedDev(ctx, f.db)
	if err != nil || d != 5 || r != 3 {
		t.Fatalf("seedDev = %d, %d, %v; want 5, 3", d, r, err)
	}
	for _, c := range []struct{ id, vehicle string }{{"driver-1", "CAR"}, {"driver-3", "CAR"}, {"driver-4", "BIKE"}, {"driver-5", "BIKE"}} {
		r := f.call("GET", "/drivers/"+c.id, auth.Driver, c.id, nil)
		if r.body["status"] != "APPROVED" || r.body["vehicleType"] != c.vehicle || r.body["plate"] == "" || r.body["name"] == "" {
			t.Errorf("%s: %s", c.id, r.raw)
		}
		wantStatus(t, c.id+" public", f.call("GET", "/drivers/"+c.id+"/public", auth.Rider, "rider-1", nil), 200)
	}
	r2 := f.call("GET", "/riders/rider-2", auth.Rider, "rider-2", nil)
	if r2.body["name"] == "" {
		t.Errorf("rider-2: %s", r2.raw)
	}
	if n := f.count(`SELECT count(*) FROM outbox WHERE source = 'profile-service'`); n != 0 {
		t.Errorf("the seed queued %d event(s)", n)
	}

	// A second run adds nothing and undoes nothing.
	wantStatus(t, "suspend a seeded driver", f.decide("suspend", "driver-2", "test"), 200)
	f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Changed"})
	d, r, err = postgres.SeedDev(ctx, f.db)
	if err != nil || d != 0 || r != 0 {
		t.Errorf("second seedDev = %d, %d, %v; want 0, 0", d, r, err)
	}
	if got := f.statusOf("driver-2"); got != "SUSPENDED" {
		t.Errorf("seeding again changed a decision: driver-2 is %s", got)
	}
	if got := f.call("GET", "/riders/rider-1", auth.Rider, "rider-1", nil).body["name"]; got != "Changed" {
		t.Errorf("seeding again undid an edit: %v", got)
	}
}
