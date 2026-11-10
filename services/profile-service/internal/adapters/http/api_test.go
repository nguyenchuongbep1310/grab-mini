package http

// The test fixture: app.Service over in-memory fakes that behave like the Postgres adapter
// (column defaults, ordering, rollback), counters kept under the Prometheus names so the
// deltas read exactly like Task 1's golden rows, and a real token issuer and verifier.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
)

// ---------------------------------------------------------------- the store ----

// memDB implements every storage port (RiderStore, DriverReader, ApplicationQueue,
// ResyncSource, ResyncLocker, UnitOfWork) over maps. err, when set, is what every call
// returns (a pool that is closed: "closed pool"). lockHeld makes TryLock refuse, as if
// another instance held the advisory lock.
type memDB struct {
	mu      sync.Mutex
	riders  map[string]domain.Rider
	drivers map[string]domain.Driver
	outbox  []string // driver ids, in queue order

	err      error
	lockHeld bool
	// onLock runs inside TryLock before the lock is taken (the outlive test blocks there).
	onLock func(ctx context.Context)
	// ctxErrs is ctx.Err() as seen by every call after onLock.
	ctxErrs []error
}

func newMemDB() *memDB {
	return &memDB{riders: map[string]domain.Rider{}, drivers: map[string]domain.Driver{}}
}

func (m *memDB) seen(ctx context.Context) { m.ctxErrs = append(m.ctxErrs, ctx.Err()) }

func (m *memDB) Rider(ctx context.Context, id string) (domain.Rider, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return domain.Rider{}, false, m.err
	}
	r, ok := m.riders[id]
	return r, ok, nil
}

func (m *memDB) SaveRider(ctx context.Context, r domain.Rider) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.riders[r.ID] = r
	return nil
}

func (m *memDB) Driver(ctx context.Context, id string) (domain.Driver, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen(ctx)
	if m.err != nil {
		return domain.Driver{}, m.err
	}
	d, ok := m.drivers[id]
	if !ok {
		return domain.Driver{}, domain.ErrNoDriver
	}
	return d, nil
}

// ByStatus is "WHERE status = $1 [AND (status_at, driver_id) > ($3, $4)] ORDER BY
// status_at, driver_id LIMIT $2".
func (m *memDB) ByStatus(ctx context.Context, status string, limit int, after *domain.Cursor) ([]domain.Driver, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	var out []domain.Driver
	for _, d := range m.drivers {
		if d.Status != status {
			continue
		}
		if after != nil && !(d.StatusAt.After(after.At) || (d.StatusAt.Equal(after.At) && d.ID > after.ID)) {
			continue
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StatusAt.Equal(out[j].StatusAt) {
			return out[i].StatusAt.Before(out[j].StatusAt)
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *memDB) ResyncableIDs(ctx context.Context, after string, limit int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen(ctx)
	if m.err != nil {
		return nil, m.err
	}
	var ids []string
	for id, d := range m.drivers {
		if domain.Resyncable(d.Status) && id > after {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (m *memDB) TryLock(ctx context.Context) (func(), bool, error) {
	if m.onLock != nil {
		m.onLock(ctx)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen(ctx)
	if m.err != nil {
		return nil, false, m.err
	}
	if m.lockHeld {
		return nil, false, nil
	}
	m.lockHeld = true
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.lockHeld = false
	}, true, nil
}

// Do runs fn holding the store, and puts the drivers and the outbox back when it fails.
func (m *memDB) Do(ctx context.Context, fn func(app.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen(ctx)
	if m.err != nil {
		return m.err
	}
	drivers, outbox := maps.Clone(m.drivers), slices.Clone(m.outbox)
	if err := fn(memTx{m}); err != nil {
		m.drivers, m.outbox = drivers, outbox
		return err
	}
	return nil
}

type memTx struct{ m *memDB }

// LockOrCreateDriver creates the row with the column defaults (status NONE, status_at and
// updated_at now) and returns it.
func (t memTx) LockOrCreateDriver(_ context.Context, id string, now time.Time) (domain.Driver, error) {
	d, ok := t.m.drivers[id]
	if !ok {
		d = domain.Driver{ID: id, Status: domain.StatusNone, StatusAt: now, UpdatedAt: now}
		t.m.drivers[id] = d
	}
	return d, nil
}

func (t memTx) LockDriver(_ context.Context, id string) (domain.Driver, error) {
	d, ok := t.m.drivers[id]
	if !ok {
		return domain.Driver{}, domain.ErrNoDriver
	}
	return d, nil
}

func (t memTx) UpdateProfile(_ context.Context, d domain.Driver) error {
	t.m.drivers[d.ID] = d
	return nil
}

func (t memTx) MarkPending(_ context.Context, id string, now time.Time) error {
	d := t.m.drivers[id]
	d.Status, d.Reason, d.StatusAt, d.UpdatedAt = domain.StatusPending, "", now, now
	t.m.drivers[id] = d
	return nil
}

func (t memTx) RecordReview(_ context.Context, d domain.Driver, at time.Time) error {
	old := t.m.drivers[d.ID]
	old.Status, old.Reason, old.ReviewedBy = d.Status, d.Reason, d.ReviewedBy
	old.ReviewedAt = &at
	old.StatusAt, old.UpdatedAt = d.StatusAt, d.StatusAt
	t.m.drivers[d.ID] = old
	return nil
}

func (t memTx) QueueReview(_ context.Context, driverID string, _ []byte) error {
	t.m.outbox = append(t.m.outbox, driverID)
	return nil
}

// --------------------------------------------------------------- encoders ----

type jsonEncoder struct{}

func (jsonEncoder) Reviewed(_ context.Context, e app.ReviewedEvent) ([]byte, error) {
	return json.Marshal(e)
}

type failingEncoder struct{}

func (failingEncoder) Reviewed(context.Context, app.ReviewedEvent) ([]byte, error) {
	return nil, errors.New("schema registry down")
}

// switchEncoder delegates to the fixture's current encoder, so a test can swap it without
// rebuilding the service.
type switchEncoder struct{ f *fixture }

func (s switchEncoder) Reviewed(ctx context.Context, e app.ReviewedEvent) ([]byte, error) {
	return s.f.enc.Reviewed(ctx, e)
}

// --------------------------------------------------------------- counters ----

// counters implements app.Metrics and Refusals. Values are kept under the Prometheus
// series names of adapters/metrics, so a delta reads like Task 1's; calls is the order.
type counters struct {
	mu     sync.Mutex
	values map[string]float64
	calls  []string
}

func newCounters() *counters { return &counters{values: map[string]float64{}} }

func (c *counters) add(name, label, value string, n float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%s{%s=%q}", name, label, value)
	c.values[key] += n
	c.calls = append(c.calls, key)
}

func (c *counters) Updated(kind string) { c.add("profile_updates_total", "kind", kind, 1) }
func (c *counters) Application(result string) {
	c.add("profile_applications_total", "result", result, 1)
}
func (c *counters) Review(decision string) { c.add("profile_reviews_total", "decision", decision, 1) }
func (c *counters) PublicLookup(result string) {
	c.add("profile_public_lookups_total", "result", result, 1)
}
func (c *counters) ResyncEvents(status string, n int) {
	c.add("profile_resync_events_total", "status", status, float64(n))
}
func (c *counters) ResyncRun(result string) { c.add("profile_resync_runs_total", "result", result, 1) }
func (c *counters) Refused(code string)     { c.add("profile_requests_rejected_total", "reason", code, 1) }

func (c *counters) snapshot() map[string]float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.values)
}

func (c *counters) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

// gDelta is Task 1's: "series=delta" for every series that moved, sorted.
func gDelta(before, after map[string]float64) string {
	var parts []string
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			parts = append(parts, fmt.Sprintf("%s=%v", k, d))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// ---------------------------------------------------------------- fixture ----

type fixture struct {
	t        *testing.T
	db       *memDB
	ctr      *counters
	enc      app.Encoder
	now      time.Time
	issuer   *auth.Issuer
	verifier *auth.Verifier
	limiter  *auth.Limiter
	api      http.Handler
}

type result struct {
	status int
	header http.Header
	body   map[string]any
	raw    string
}

// newFixture is a working service with a frozen clock and no limiter.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := auth.NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{
		t: t, db: newMemDB(), ctr: newCounters(), enc: jsonEncoder{}, now: goldenNow,
		issuer: issuer, verifier: auth.NewVerifier(auth.NewKeySet(pub)),
	}
	f.rebuild()
	return f
}

// offlineFixture answers every storage call with "closed pool", like Task 1's closed pool.
func offlineFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.db.err = errors.New("closed pool")
	return f
}

func (f *fixture) rebuild() {
	svc := app.New(app.Deps{
		Riders: f.db, Drivers: f.db, Queue: f.db, Resync: f.db, Locker: f.db, UoW: f.db,
		Encoder: switchEncoder{f}, Metrics: f.ctr, Now: func() time.Time { return f.now },
	})
	f.api = NewAPI(svc, f.verifier, f.limiter, f.ctr).Routes()
}

func (f *fixture) useLimiter(l *auth.Limiter) { f.limiter = l; f.rebuild() }

func (f *fixture) request(method, path string, role auth.Role, subject string, body any) *http.Request {
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
	return req
}

func (f *fixture) serve(req *http.Request) result {
	rec := httptest.NewRecorder()
	f.api.ServeHTTP(rec, req)
	res := result{status: rec.Code, header: rec.Header(), body: map[string]any{}, raw: rec.Body.String()}
	json.Unmarshal(rec.Body.Bytes(), &res.body)
	return res
}

func (f *fixture) call(method, path string, role auth.Role, subject string, body any) result {
	f.t.Helper()
	return f.serve(f.request(method, path, role, subject, body))
}

func wantStatus(t *testing.T, what string, r result, status int) {
	t.Helper()
	if r.status != status {
		t.Errorf("%s: status %d, want %d (%s)", what, r.status, status, r.raw)
	}
}

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

func (f *fixture) resync(body any) result {
	return f.call("POST", "/driver-applications/resync", auth.Admin, "ops-1", body)
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

// gCapture sends the standard logger to a buffer with no timestamp.
func gCapture(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf syncBuffer
	flags, out := log.Flags(), log.Writer()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(out); log.SetFlags(flags) })
	return &buf.Buffer
}

// syncBuffer is a bytes.Buffer the logger writes under a lock (the logger already
// serialises writes; the type only exists so -race sees the same discipline in tests).
type syncBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

// ------------------------------------------------------------------ tests ----

const (
	bodyNotFound = "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n"
	bodyBadJSON  = "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"
)

// Each error a use case returns, and each handler's 500, answers as the flat handler did; a
// 500 logs "<label>: <err>" and nothing else, and counts no refusal.
func TestEveryAppErrorMapsToItsOldResponse(t *testing.T) {
	rd, drv, adm := auth.Rider, auth.Driver, auth.Admin
	boom := errors.New("read tcp: connection reset (SQLSTATE 08006)")
	broken := func(f *fixture) { f.db.err = boom }
	cases := []struct {
		name               string
		setup              func(*fixture)
		method, path       string
		role               auth.Role
		sub                string
		body               any
		status             int
		want, log, counted string
	}{
		{"FieldError on a rider is 400 invalid_profile with field", nil, "PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": "Bảo", "phone": "abc"}, 400,
			"{\"code\":\"invalid_profile\",\"error\":\"phone: may contain only digits, spaces, + - . ( )\",\"field\":\"phone\"}\n", "", `profile_requests_rejected_total{reason="invalid_profile"}=1`},
		{"FieldError on a driver is 400 invalid_profile with field", nil, "PUT", "/drivers/driver-1", drv, "driver-1", map[string]any{"name": "Bảo", "color": strings.Repeat("ạ", 31)}, 400,
			"{\"code\":\"invalid_profile\",\"error\":\"color: must be at most 30 characters (got 31)\",\"field\":\"color\"}\n", "", `profile_requests_rejected_total{reason="invalid_profile"}=1`},
		{"FieldError on a review is 400 invalid_profile with field", nil, "POST", "/drivers/driver-1/suspend", adm, "ops-1", nil, 400,
			"{\"code\":\"invalid_profile\",\"error\":\"reason: is required\",\"field\":\"reason\"}\n", "", `profile_requests_rejected_total{reason="invalid_profile"}=1`},
		{"IncompleteError on PUT /drivers is 400 invalid_profile", func(f *fixture) { f.applied("driver-1") }, "PUT", "/drivers/driver-1", drv, "driver-1", map[string]any{"name": "Bảo"}, 400,
			"{\"code\":\"invalid_profile\",\"error\":\"an application under review needs: vehicleType, plate\",\"field\":\"vehicleType\"}\n", "", `profile_requests_rejected_total{reason="invalid_profile"}=1`},
		{"IncompleteError on apply is 422 incomplete_profile", func(f *fixture) { f.putDriver("driver-1", map[string]any{"name": "Bảo", "vehicleType": "CAR"}) }, "POST", "/drivers/driver-1/application", drv, "driver-1", nil, 422,
			"{\"code\":\"incomplete_profile\",\"error\":\"fill in before applying: plate\",\"field\":\"plate\"}\n", "", `profile_requests_rejected_total{reason="incomplete_profile"}=1`},
		{"ErrNoProfile on apply is 422 incomplete_profile, field name", nil, "POST", "/drivers/driver-1/application", drv, "driver-1", nil, 422,
			"{\"code\":\"incomplete_profile\",\"error\":\"save your profile (name, vehicle type, plate) before applying\",\"field\":\"name\"}\n", "", `profile_requests_rejected_total{reason="incomplete_profile"}=1`},
		{"ErrSuspended on apply is 409 suspended", func(f *fixture) { f.approved("driver-1"); f.decide("suspend", "driver-1", "x") }, "POST", "/drivers/driver-1/application", drv, "driver-1", nil, 409,
			"{\"code\":\"suspended\",\"error\":\"your account is suspended; contact support\",\"status\":\"SUSPENDED\"}\n", "", `profile_requests_rejected_total{reason="suspended"}=1`},
		{"TransitionError on a review is 409 invalid_transition with status", func(f *fixture) { f.applied("driver-1") }, "POST", "/drivers/driver-1/suspend", adm, "ops-1", map[string]string{"reason": "x"}, 409,
			"{\"code\":\"invalid_transition\",\"error\":\"a driver who is PENDING cannot be SUSPENDED\",\"status\":\"PENDING\"}\n", "", `profile_requests_rejected_total{reason="invalid_transition"}=1`},
		{"ErrNoDriver on a review is 404", nil, "POST", "/drivers/nobody/approve", adm, "ops-1", nil, 404, bodyNotFound, "", `profile_requests_rejected_total{reason="not_found"}=1`},
		{"ErrNoDriver on the public view is 404", nil, "GET", "/drivers/nobody/public", rd, "rider-1", nil, 404, bodyNotFound, "",
			`profile_public_lookups_total{result="not_found"}=1 profile_requests_rejected_total{reason="not_found"}=1`},
		{"ErrNoDriver on a resync is 404 and no run", nil, "POST", "/driver-applications/resync", adm, "ops-1", map[string]string{"driverId": "nobody"}, 404, bodyNotFound, "", `profile_requests_rejected_total{reason="not_found"}=1`},
		{"ErrNoDriver on GET /drivers is the NONE profile, not an error", nil, "GET", "/drivers/driver-1", drv, "driver-1", nil, 200,
			"{\"driverId\":\"driver-1\",\"name\":\"\",\"phone\":\"\",\"vehicleType\":\"\",\"plate\":\"\",\"model\":\"\",\"color\":\"\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":null,\"updatedAt\":null}\n", "", ""},
		{"InvalidRequestError on the list is 400 invalid_request", nil, "GET", "/driver-applications?limit=101", adm, "ops-1", nil, 400,
			"{\"code\":\"invalid_request\",\"error\":\"limit must be from 1 to 100\"}\n", "", `profile_requests_rejected_total{reason="invalid_request"}=1`},
		{"ErrResyncRunning is 409 resync_running", func(f *fixture) { f.db.lockHeld = true }, "POST", "/driver-applications/resync", adm, "ops-1", nil, 409,
			"{\"code\":\"resync_running\",\"error\":\"a resync is already running; try again when it has finished\"}\n", "",
			`profile_requests_rejected_total{reason="resync_running"}=1 profile_resync_runs_total{result="refused_running"}=1`},
		{"an encode failure on a vehicle change is a 500 of save driver", func(f *fixture) { f.approved("driver-1"); f.enc = failingEncoder{} }, "PUT", "/drivers/driver-1", drv, "driver-1", vehicle("30G-246.80"), 500,
			gInternal, "save driver driver-1: encoding the review result: schema registry down\n", ""},
		{"an encode failure on a review is a 500 of <action> driver", func(f *fixture) { f.applied("driver-1"); f.enc = failingEncoder{} }, "POST", "/drivers/driver-1/reject", adm, "ops-1", map[string]string{"reason": "x"}, 500,
			gInternal, "reject driver driver-1: encoding the review result: schema registry down\n", ""},
		{"500 load rider", broken, "GET", "/riders/rider-1", rd, "rider-1", nil, 500, gInternal, "load rider rider-1: " + boom.Error() + "\n", ""},
		{"500 save rider", broken, "PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": "Bảo"}, 500, gInternal, "save rider rider-1: " + boom.Error() + "\n", ""},
		{"500 load driver", broken, "GET", "/drivers/driver-1", drv, "driver-1", nil, 500, gInternal, "load driver driver-1: " + boom.Error() + "\n", ""},
		{"500 save driver", broken, "PUT", "/drivers/driver-1", drv, "driver-1", vehicle("51A-123.45"), 500, gInternal, "save driver driver-1: " + boom.Error() + "\n", ""},
		{"500 apply driver", broken, "POST", "/drivers/driver-1/application", drv, "driver-1", nil, 500, gInternal, "apply driver driver-1: " + boom.Error() + "\n", ""},
		{"500 load public driver has no id", broken, "GET", "/drivers/driver-7/public", rd, "rider-1", nil, 500, gInternal, "load public driver: " + boom.Error() + "\n", ""},
		{"500 list driver applications", broken, "GET", "/driver-applications?status=APPROVED", adm, "ops-1", nil, 500, gInternal, "list driver applications: " + boom.Error() + "\n", ""},
		{"500 resync", broken, "POST", "/driver-applications/resync", adm, "ops-1", nil, 500, gInternal, "resync: " + boom.Error() + "\n", `profile_resync_runs_total{result="failed"}=1`},
		{"500 approve driver", broken, "POST", "/drivers/driver-1/approve", adm, "ops-1", nil, 500, gInternal, "approve driver driver-1: " + boom.Error() + "\n", ""},
		{"500 reject driver", broken, "POST", "/drivers/driver-1/reject", adm, "ops-1", map[string]string{"reason": "x"}, 500, gInternal, "reject driver driver-1: " + boom.Error() + "\n", ""},
		{"500 suspend driver", broken, "POST", "/drivers/driver-1/suspend", adm, "ops-1", map[string]string{"reason": "x"}, 500, gInternal, "suspend driver driver-1: " + boom.Error() + "\n", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf := gCapture(t)
			f := newFixture(t)
			if c.setup != nil {
				c.setup(f)
			}
			buf.Reset()
			before := f.ctr.snapshot()
			res := f.call(c.method, c.path, c.role, c.sub, c.body)
			wantStatus(t, c.name, res, c.status)
			gEq(t, "body", res.raw, c.want)
			gEq(t, "Content-Type", res.header.Get("Content-Type"), "application/json")
			gEq(t, "Cache-Control", res.header.Get("Cache-Control"), "no-store")
			gEq(t, "log", buf.String(), c.log)
			gEq(t, "counters", gDelta(before, f.ctr.snapshot()), c.counted)
		})
	}
}

// The route set and roles of the flat routes(): no token is 401, a wrong role 403 (both from
// pkg/auth, without Cache-Control), an allowed role reaches the handler; anything else is the
// mux's 404 or 405.
func TestRoutesAndRoles(t *testing.T) {
	gCapture(t)
	f := newFixture(t)
	all := []auth.Role{auth.Rider, auth.Driver, auth.Admin}
	routes := []struct {
		method, path string
		allowed      []auth.Role
	}{
		{"GET", "/riders/x", []auth.Role{auth.Rider}},
		{"PUT", "/riders/x", []auth.Role{auth.Rider}},
		{"GET", "/drivers/x", []auth.Role{auth.Driver}},
		{"PUT", "/drivers/x", []auth.Role{auth.Driver}},
		{"POST", "/drivers/x/application", []auth.Role{auth.Driver}},
		{"GET", "/drivers/x/public", all},
		{"GET", "/driver-applications", []auth.Role{auth.Admin}},
		{"POST", "/driver-applications/resync", []auth.Role{auth.Admin}},
		{"POST", "/drivers/x/approve", []auth.Role{auth.Admin}},
		{"POST", "/drivers/x/reject", []auth.Role{auth.Admin}},
		{"POST", "/drivers/x/suspend", []auth.Role{auth.Admin}},
	}
	for _, r := range routes {
		res := f.call(r.method, r.path, "", "", nil)
		wantStatus(t, "no token "+r.method+" "+r.path, res, 401)
		gEq(t, "no token body", res.raw, "{\"error\":\"missing bearer token\"}\n")
		gEq(t, "no token Cache-Control", res.header.Get("Cache-Control"), "")
		for _, role := range all {
			res := f.call(r.method, r.path, role, "x", map[string]string{"reason": "r"})
			if slices.Contains(r.allowed, role) {
				if res.status == 401 || (res.status == 403 && res.raw == "{\"error\":\"this role can't do that\"}\n") {
					t.Errorf("%s on %s %s: refused by the verifier: %d %s", role, r.method, r.path, res.status, res.raw)
				}
				continue
			}
			wantStatus(t, string(role)+" on "+r.method+" "+r.path, res, 403)
			gEq(t, "wrong role body", res.raw, "{\"error\":\"this role can't do that\"}\n")
		}
	}
	health := f.call("GET", "/health", "", "", nil)
	wantStatus(t, "health", health, 200)
	gEq(t, "health", health.raw, "{\"status\":\"ok\"}\n")
	wantStatus(t, "unknown action", f.call("POST", "/drivers/x/unsuspend", auth.Admin, "ops-1", nil), 404)
	wantStatus(t, "unknown method", f.call("DELETE", "/riders/x", auth.Rider, "x", nil), 405)
}

func TestOwnProfileRuleAnswers403WhetherOrNotTheProfileExists(t *testing.T) {
	gCapture(t)
	const forbidden = "{\"code\":\"forbidden\",\"error\":\"you can only use your own profile\"}\n"
	requests := []struct {
		method, path string
		role         auth.Role
		sub          string
		body         any
	}{
		{"GET", "/riders/rider-1", auth.Rider, "rider-9", nil},
		{"PUT", "/riders/rider-1", auth.Rider, "rider-9", `{`}, // 403 before the JSON 400
		{"GET", "/drivers/driver-1", auth.Driver, "driver-9", nil},
		{"PUT", "/drivers/driver-1", auth.Driver, "driver-9", map[string]string{"name": ""}}, // before field 400s
		{"POST", "/drivers/driver-1/application", auth.Driver, "driver-9", nil},
	}
	for _, exists := range []bool{false, true} {
		f := newFixture(t)
		if exists {
			f.approved("driver-1")
			wantStatus(t, "rider", f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Bảo"}), 200)
		}
		for _, r := range requests {
			before := f.ctr.snapshot()
			res := f.call(r.method, r.path, r.role, r.sub, r.body)
			wantStatus(t, fmt.Sprintf("%s %s (exists %v)", r.method, r.path, exists), res, 403)
			gEq(t, "body", res.raw, forbidden)
			gEq(t, "counted", gDelta(before, f.ctr.snapshot()), `profile_requests_rejected_total{reason="forbidden"}=1`)
		}
	}
}

func TestThePublicRouteIsLimitedPerCallerAndRefusalsAreCounted(t *testing.T) {
	gCapture(t)
	f := newFixture(t)
	f.approved("driver-1")
	f.useLimiter(auth.NewLimiter(1.0/60, 3))
	for i := 0; i < 3; i++ {
		wantStatus(t, "burst", f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil), 200)
	}
	for i := 0; i < 2; i++ {
		before := f.ctr.snapshot()
		res := f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil)
		wantStatus(t, "over the burst", res, 429)
		gEq(t, "Retry-After", res.header.Get("Retry-After"), "60")
		gEq(t, "body", res.raw, "{\"code\":\"rate_limited\",\"error\":\"too many lookups, slow down\",\"retryAfter\":60}\n")
		gEq(t, "Cache-Control", res.header.Get("Cache-Control"), "no-store")
		// Counted once per refusal, and the lookup itself never ran.
		gEq(t, "counted", gDelta(before, f.ctr.snapshot()), `profile_requests_rejected_total{reason="rate_limited"}=1`)
	}
	// The key is role:subject: another caller, and the same subject in another role, still pass.
	wantStatus(t, "another rider", f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-2", nil), 200)
	wantStatus(t, "same subject, another role", f.call("GET", "/drivers/driver-1/public", auth.Driver, "rider-1", nil), 200)
}

func TestANilLimiterLetsEverythingThrough(t *testing.T) {
	gCapture(t)
	f := newFixture(t)
	f.approved("driver-1")
	for i := 0; i < 100; i++ {
		if res := f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil); res.status != 200 {
			t.Fatalf("lookup %d: %d %s", i, res.status, res.raw)
		}
	}
}

func TestNewPublicLimiterIsTheOldOne(t *testing.T) {
	t.Setenv(auth.RateLimitScaleEnv, "0")
	if NewPublicLimiter(60) != nil {
		t.Error("RATE_LIMIT_SCALE=0: want no limiter")
	}
	t.Setenv(auth.RateLimitScaleEnv, "")
	l := NewPublicLimiter(8) // burst max(1, 8/4) = 2, then 8 a minute
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("k"); !ok {
			t.Fatalf("burst lookup %d refused", i)
		}
	}
	ok, wait := l.Allow("k")
	if ok || wait <= 0 || wait > 8*time.Second {
		t.Errorf("third lookup: ok %v, wait %v; want refused, about 7.5s", ok, wait)
	}
	if ok, _ := NewPublicLimiter(3).Allow("k"); !ok { // burst max(1, 0) = 1
		t.Error("perMinute 3: want a burst of 1")
	}
}

func TestTheReviewRoutesCoverExactlyTheDomainActions(t *testing.T) {
	f := newFixture(t)
	var mux = f.api.(*http.ServeMux)
	for _, action := range domain.ActionNames() {
		req := httptest.NewRequest("POST", "/drivers/x/"+action, nil)
		if _, pattern := mux.Handler(req); pattern != "POST /drivers/{id}/"+action {
			t.Errorf("%s: pattern %q", action, pattern)
		}
	}
	if got := domain.ActionNames(); !slices.Equal(got, []string{"approve", "reject", "suspend"}) {
		t.Errorf("actions %v", got)
	}
	for _, other := range []string{"unsuspend", "application2", "review"} {
		req := httptest.NewRequest("POST", "/drivers/x/"+other, nil)
		if _, pattern := mux.Handler(req); pattern != "" {
			t.Errorf("%s: registered as %q", other, pattern)
		}
	}
}

func TestOnlyReviewAndResyncAcceptAnEmptyBody(t *testing.T) {
	gCapture(t)
	f := newFixture(t)
	f.applied("driver-1")
	// Empty bodies.
	wantStatus(t, "approve, empty", f.call("POST", "/drivers/driver-1/approve", auth.Admin, "ops-1", nil), 200)
	wantStatus(t, "resync, empty", f.call("POST", "/driver-applications/resync", auth.Admin, "ops-1", nil), 200)
	for _, r := range []struct {
		method, path string
		role         auth.Role
		sub          string
	}{
		{"PUT", "/riders/rider-1", auth.Rider, "rider-1"},
		{"PUT", "/drivers/driver-2", auth.Driver, "driver-2"},
	} {
		res := f.call(r.method, r.path, r.role, r.sub, "")
		wantStatus(t, r.path+" empty", res, 400)
		gEq(t, r.path+" empty", res.raw, bodyBadJSON)
	}
	// Broken JSON is refused by all four that read a body.
	for _, path := range []string{"/drivers/driver-1/suspend", "/driver-applications/resync"} {
		res := f.call("POST", path, auth.Admin, "ops-1", `{`)
		gEq(t, path+" broken", res.raw, bodyBadJSON)
	}
	// A body over 8 KiB is cut and so is not JSON.
	big := `{"name":"` + strings.Repeat("a", 9<<10) + `"}`
	gEq(t, "over 8 KiB", f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", big).raw, bodyBadJSON)
}

func TestViewsKeepTheirFieldOrderNullsAndTheNoneFallback(t *testing.T) {
	enc := func(v any) string {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	gEq(t, "rider never saved", enc(newRiderView(domain.Rider{ID: "r"})), `{"riderId":"r","name":"","phone":"","updatedAt":null}`)
	gEq(t, "rider", enc(newRiderView(domain.Rider{ID: "r", Name: "N", Phone: "P", UpdatedAt: at})), `{"riderId":"r","name":"N","phone":"P","updatedAt":"2026-03-04T05:06:07Z"}`)
	gEq(t, "driver with no status is NONE, times null", enc(newDriverView(domain.Driver{ID: "d", StatusAt: at})),
		`{"driverId":"d","name":"","phone":"","vehicleType":"","plate":"","model":"","color":"","status":"NONE","reason":"","reviewedAt":null,"statusAt":null,"updatedAt":null}`)
	d := domain.Driver{ID: "d", Name: "N", Phone: "P", Vehicle: domain.VehicleBike, Plate: "PL", Model: "M", Color: "C",
		Status: domain.StatusApproved, Reason: "R", ReviewedBy: "ops-1", ReviewedAt: &at, StatusAt: at, UpdatedAt: at.Add(time.Second)}
	gEq(t, "driver", enc(newDriverView(d)),
		`{"driverId":"d","name":"N","phone":"P","vehicleType":"BIKE","plate":"PL","model":"M","color":"C","status":"APPROVED","reason":"R","reviewedAt":"2026-03-04T05:06:07Z","statusAt":"2026-03-04T05:06:07Z","updatedAt":"2026-03-04T05:06:08Z"}`)
	gEq(t, "admin adds reviewedBy last", enc(newAdminDriverView(d)),
		`{"driverId":"d","name":"N","phone":"P","vehicleType":"BIKE","plate":"PL","model":"M","color":"C","status":"APPROVED","reason":"R","reviewedAt":"2026-03-04T05:06:07Z","statusAt":"2026-03-04T05:06:07Z","updatedAt":"2026-03-04T05:06:08Z","reviewedBy":"ops-1"}`)
	gEq(t, "public: no phone, no status", enc(newPublicDriverView(d)), `{"driverId":"d","name":"N","vehicleType":"BIKE","plate":"PL","model":"M","color":"C"}`)
}

func TestTheAdminListIsNeverNullAndCarriesNextCursor(t *testing.T) {
	gCapture(t)
	f := newFixture(t)
	gEq(t, "empty", f.call("GET", "/driver-applications", auth.Admin, "ops-1", nil).raw, "{\"applications\":[],\"nextCursor\":null}\n")
	f.applied("driver-1")
	f.now = f.now.Add(time.Minute)
	f.applied("driver-2")
	page := f.call("GET", "/driver-applications?status=PENDING&limit=1", auth.Admin, "ops-1", nil)
	if page.body["nextCursor"] != "1772600767000000.driver-1" {
		t.Errorf("nextCursor %v", page.body["nextCursor"])
	}
	last := f.call("GET", "/driver-applications?limit=1&cursor=1772600767000000.driver-1", auth.Admin, "ops-1", nil)
	if last.body["nextCursor"] != nil || !strings.Contains(last.raw, `"driverId":"driver-2"`) || !strings.HasSuffix(last.raw, ",\"nextCursor\":null}\n") {
		t.Errorf("last page %s", last.raw)
	}
	// An empty cursor is no cursor (domain.ParseCursor would refuse "").
	wantStatus(t, "empty cursor", f.call("GET", "/driver-applications?cursor=", auth.Admin, "ops-1", nil), 200)
}

func TestTheResyncRunOutlivesACancelledRequest(t *testing.T) {
	buf := gCapture(t)
	f := newFixture(t)
	f.approved("driver-1")
	entered, release := make(chan struct{}), make(chan struct{})
	var deadline time.Time
	var hasDeadline bool
	f.db.onLock = func(ctx context.Context) {
		deadline, hasDeadline = ctx.Deadline()
		close(entered)
		<-release
	}
	f.db.ctxErrs = nil
	buf.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	req := f.request("POST", "/driver-applications/resync", auth.Admin, "ops-1", nil).WithContext(ctx)
	done := make(chan result)
	go func() { done <- f.serve(req) }()
	<-entered
	cancel()
	close(release)
	res := <-done
	wantStatus(t, "resync", res, 200)
	gEq(t, "body", res.raw, "{\"counts\":{\"APPROVED\":1,\"REJECTED\":0,\"SUSPENDED\":0},\"queued\":1,\"skipped\":0}\n")
	gEq(t, "log", buf.String(), "resync by ops-1: all drivers: 1 event(s) queued (APPROVED 1, REJECTED 0, SUSPENDED 0, 0 skipped)\n")
	if len(f.db.ctxErrs) == 0 {
		t.Fatal("the use case reached no port")
	}
	for i, err := range f.db.ctxErrs {
		if err != nil {
			t.Errorf("port call %d saw a dead context: %v", i, err)
		}
	}
	if !hasDeadline || time.Until(deadline) < app.ResyncTimeout-time.Minute || time.Until(deadline) > app.ResyncTimeout {
		t.Errorf("deadline %v (set %v), want about now + %v", deadline, hasDeadline, app.ResyncTimeout)
	}
}

func TestEveryResponseCarriesNoStore(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	off := offlineFixture(t)
	gCapture(t)
	rd, drv, adm := auth.Rider, auth.Driver, auth.Admin
	checks := []struct {
		f            *fixture
		method, path string
		role         auth.Role
		sub          string
		body         any
	}{
		{f, "GET", "/health", "", "", nil},
		{f, "GET", "/riders/rider-1", rd, "rider-1", nil},
		{f, "PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": "Bảo"}},
		{f, "PUT", "/riders/rider-1", rd, "rider-1", `{`},
		{f, "GET", "/riders/rider-2", rd, "rider-1", nil},
		{f, "GET", "/drivers/driver-1", drv, "driver-1", nil},
		{f, "PUT", "/drivers/driver-2", drv, "driver-2", vehicle("51A-123.45")},
		{f, "POST", "/drivers/driver-2/application", drv, "driver-2", nil},
		{f, "POST", "/drivers/driver-3/application", drv, "driver-3", nil},
		{f, "GET", "/drivers/driver-1/public", rd, "rider-1", nil},
		{f, "GET", "/drivers/nobody/public", rd, "rider-1", nil},
		{f, "GET", "/driver-applications", adm, "ops-1", nil},
		{f, "GET", "/driver-applications?limit=0", adm, "ops-1", nil},
		{f, "POST", "/drivers/driver-2/reject", adm, "ops-1", map[string]string{"reason": "no"}},
		{f, "POST", "/drivers/driver-2/reject", adm, "ops-1", map[string]string{"reason": "no"}},
		{f, "POST", "/driver-applications/resync", adm, "ops-1", nil},
		{f, "POST", "/driver-applications/resync", adm, "ops-1", map[string]string{"driverId": "nobody"}},
		{off, "GET", "/riders/rider-1", rd, "rider-1", nil},
		{off, "POST", "/driver-applications/resync", adm, "ops-1", nil},
	}
	for _, c := range checks {
		res := c.f.call(c.method, c.path, c.role, c.sub, c.body)
		what := fmt.Sprintf("%s %s -> %d", c.method, c.path, res.status)
		gEq(t, what+" Cache-Control", res.header.Get("Cache-Control"), "no-store")
		gEq(t, what+" Content-Type", res.header.Get("Content-Type"), "application/json")
	}
}

// A public 404 counts the lookup (use case) before the refusal (adapter).
func TestAPublicNotFoundCountsTheLookupThenTheRefusal(t *testing.T) {
	gCapture(t)
	f := newFixture(t)
	f.call("GET", "/drivers/nobody/public", auth.Rider, "rider-1", nil)
	want := []string{`profile_public_lookups_total{result="not_found"}`, `profile_requests_rejected_total{reason="not_found"}`}
	if got := f.ctr.order(); !slices.Equal(got, want) {
		t.Errorf("order %v, want %v", got, want)
	}
}

// No log line of the adapter carries a name, phone, plate, model, colour or reason.
func TestTheAdapterLogsNoPersonalData(t *testing.T) {
	buf := gCapture(t)
	f := newFixture(t)
	secret := map[string]any{"name": "Zebulon Quixote", "phone": "0987 654 321", "vehicleType": "CAR", "plate": "99Z-999.99", "model": "Secretmobile", "color": "Ultraviolet"}
	f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Zebulon Quixote", "phone": "0987 654 321"})
	f.putDriver("driver-1", secret)
	f.apply("driver-1")
	f.decide("approve", "driver-1", "Hushhush reason")
	secret["plate"] = "88Y-888.88"
	f.putDriver("driver-1", secret)
	f.decide("approve", "driver-1", "")
	f.decide("suspend", "driver-1", "Hushhush reason")
	f.call("GET", "/drivers/driver-1/public", auth.Rider, "rider-1", nil)
	f.call("GET", "/driver-applications?status=SUSPENDED", auth.Admin, "ops-1", nil)
	f.resync(nil)
	f.enc = failingEncoder{}
	f.decide("approve", "driver-1", "Hushhush reason")
	out := buf.String()
	if out == "" {
		t.Fatal("nothing logged")
	}
	for _, s := range []string{"Zebulon", "0987", "99Z", "88Y", "Secretmobile", "Ultraviolet", "Hushhush"} {
		if strings.Contains(out, s) {
			t.Errorf("log carries %q:\n%s", s, out)
		}
	}
}
