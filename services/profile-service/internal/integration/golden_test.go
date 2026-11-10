package integration

// Characterisation ("golden") tests, written against the flat profile-service before the
// Clean Architecture migration and moved here with it. Every expectation below was recorded
// by running the unchanged flat code, not derived from the specification: the point is to
// prove the migration changes no byte a client, an operator or Matching can see. They go
// through the HTTP adapter wired as main wires it and, where a row needs state, the real
// Postgres adapters on the test's own database, exactly like integration_test.go. Rows that
// never reach the database use a pool that is closed at once, so a row that unexpectedly
// reaches it answers 500 and fails loudly.
//
// adapters/http keeps the same tables over in-memory fakes; this copy is what proves the
// fakes answer what Postgres answers (success bodies, the closed-pool 500 lines) and is the
// home of the counter (real registry), outbox and log rows.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"grab-mini/pkg/auth"
	"grab-mini/profile-service/internal/adapters/postgres"
)

// goldenNow is the frozen clock of every golden test that touches the database.
var goldenNow = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

// ------------------------------------------------------------------ helpers ----

// gFix is newFixture with a clock that does not depend on when the test runs.
func gFix(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.now = goldenNow
	return f
}

// gOffline is a fixture whose pool was closed before it was ever used: handlers that stop
// before the database never notice, and the ones that reach it answer 500 "closed pool".
func gOffline(t *testing.T) *fixture {
	t.Helper()
	db, err := pgxpool.New(context.Background(), "postgres://nobody:nobody@127.0.0.1:1/none")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	f := newFixtureOn(t, db)
	f.now = goldenNow
	return f
}

func gEq(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %q, want %q", name, got, want)
	}
}

// gCapture sends the standard logger to a buffer with no timestamp.
func gCapture(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	flags, out := log.Flags(), log.Writer()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(out); log.SetFlags(flags) })
	return &buf
}

// State builders for rows that need a driver in some status.
func gSaved(f *fixture)    { wantStatus(f.t, "put", f.putDriver("driver-1", vehicle("51A-123.45")), 200) }
func gPending(f *fixture)  { f.applied("driver-1") }
func gApproved(f *fixture) { f.approved("driver-1") }
func gSuspended(f *fixture) {
	f.approved("driver-1")
	wantStatus(f.t, "suspend", f.decide("suspend", "driver-1", "complaints"), 200)
}
func gRejected(f *fixture) {
	f.applied("driver-1")
	wantStatus(f.t, "reject", f.decide("reject", "driver-1", "documents unreadable"), 200)
}
func gRiderSaved(f *fixture) {
	wantStatus(f.t, "put rider", f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Bảo"}), 200)
}

const (
	gJSON    = "application/json"
	gNoStore = "no-store"

	gInternal       = `{"code":"internal","error":"internal error"}` + "\n"
	gUnauth         = `{"error":"missing bearer token"}` + "\n"
	gWrongRole      = `{"error":"this role can't do that"}` + "\n"
	gReasonRequired = `{"code":"invalid_profile","error":"reason: is required","field":"reason"}` + "\n"
	gReasonTooLong  = `{"code":"invalid_profile","error":"reason: must be at most 500 characters (got 501)","field":"reason"}` + "\n"
	gReasonControl  = `{"code":"invalid_profile","error":"reason: must not contain control characters","field":"reason"}` + "\n"
	gForbiddenOwn   = `{"code":"forbidden","error":"you can only use your own profile"}` + "\n"
)

// gErr is one row of TestErrorResponsesAreStable. A row with no setup never reaches the
// database; db() makes it use a real one, with setup (possibly nil) run first.
type gErr struct {
	name, method, path string
	role               auth.Role
	sub                string
	body               any
	status             int
	want               string
	cache              string // Cache-Control the row expects
	wwwAuth            string // WWW-Authenticate the row expects
	setup              func(*fixture)
	real               bool
	limiter            bool // four lookups on a limiter of 3: the fourth is the row
}

func e(name, req string, role auth.Role, sub string, body any, status int, want string) gErr {
	m, p, _ := strings.Cut(req, " ")
	r := gErr{name: name, method: m, path: p, role: role, sub: sub, body: body, status: status, want: want, cache: gNoStore}
	if status == 401 {
		r.wwwAuth = "Bearer"
	}
	return r
}

// denied marks a refusal by the token verifier (pkg/auth), which sets no Cache-Control.
func (r gErr) denied() gErr { r.cache = ""; return r }

func (r gErr) db(setup func(*fixture)) gErr { r.real, r.setup = true, setup; return r }

func (r gErr) limited() gErr { r.limiter = true; return r }

// rb is a JSON body for the rider and driver PUTs.
func rb(name, phone string) string {
	raw, _ := json.Marshal(map[string]string{"name": name, "phone": phone})
	return string(raw)
}

// dv is a complete driver profile with one field replaced.
func dv(field, value string) map[string]any {
	b := vehicle("51A-123.45")
	b[field] = value
	return b
}

// ------------------------------------------------------------ error responses ----

func TestErrorResponsesAreStable(t *testing.T) {
	long := strings.Repeat("x", 129)
	runes := func(n int) string { return strings.Repeat("ạ", n) }
	rid, rsub := auth.Rider, "rider-1"
	drv, dsub := auth.Driver, "driver-1"
	adm, asub := auth.Admin, "ops-1"

	var rows []gErr

	// Health needs no token.
	rows = append(rows, e("e health", "GET /health", "", "", nil, 200, `{"status":"ok"}`+"\n"))

	// Every protected route: no token is 401, a role that may not use it is 403.
	type route struct {
		req     string
		allowed []auth.Role
	}
	routes := []route{
		{"GET /riders/rider-1", []auth.Role{auth.Rider}},
		{"PUT /riders/rider-1", []auth.Role{auth.Rider}},
		{"GET /drivers/driver-1", []auth.Role{auth.Driver}},
		{"PUT /drivers/driver-1", []auth.Role{auth.Driver}},
		{"POST /drivers/driver-1/application", []auth.Role{auth.Driver}},
		{"GET /drivers/driver-1/public", []auth.Role{auth.Rider, auth.Driver, auth.Admin}},
		{"GET /driver-applications", []auth.Role{auth.Admin}},
		{"POST /driver-applications/resync", []auth.Role{auth.Admin}},
		{"POST /drivers/driver-1/approve", []auth.Role{auth.Admin}},
		{"POST /drivers/driver-1/reject", []auth.Role{auth.Admin}},
		{"POST /drivers/driver-1/suspend", []auth.Role{auth.Admin}},
	}
	for _, r := range routes {
		rows = append(rows, e("e no token "+r.req, r.req, "", "", nil, 401, gUnauth).denied())
		for _, role := range []auth.Role{auth.Rider, auth.Driver, auth.Admin} {
			ok := false
			for _, a := range r.allowed {
				ok = ok || a == role
			}
			if !ok {
				rows = append(rows, e("e "+string(role)+" on "+r.req, r.req, role, "x", nil, 403, gWrongRole).denied())
			}
		}
	}

	// Someone else's id: 403 whether or not the profile exists, same bytes.
	own := []string{"GET /riders/%s", "PUT /riders/%s", "GET /drivers/%s", "PUT /drivers/%s", "POST /drivers/%s/application"}
	for _, tpl := range own {
		role, sub, id, body := auth.Driver, "driver-9", "driver-1", any(vehicle("51A-123.45"))
		if strings.Contains(tpl, "riders") {
			role, sub, id, body = auth.Rider, "rider-9", "rider-1", map[string]string{"name": "X"}
		}
		if strings.HasPrefix(tpl, "GET") || strings.HasSuffix(tpl, "application") {
			body = nil
		}
		path := fmt.Sprintf(tpl, id)
		rows = append(rows, e("e other's profile, absent: "+path, path, role, sub, body, 403, gForbiddenOwn))
		setup := gSaved
		if strings.Contains(tpl, "riders") {
			setup = gRiderSaved
		}
		rows = append(rows, e("e other's profile, exists: "+path, path, role, sub, body, 403, gForbiddenOwn).db(setup))
	}

	// PUT /riders/{id}.
	put := func(name, body, want string) gErr {
		return e("e put rider "+name, "PUT /riders/rider-1", rid, rsub, body, 400, want)
	}
	rows = append(rows,
		put("invalid JSON", `{`, "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"),
		put("empty body", ``, "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"),
		put("name empty", rb("", ""), "{\"code\":\"invalid_profile\",\"error\":\"name: is required\",\"field\":\"name\"}\n"),
		put("name only digits", rb("12345", ""), "{\"code\":\"invalid_profile\",\"error\":\"name: must contain a letter\",\"field\":\"name\"}\n"),
		put("name 81 runes", rb(runes(81), ""), "{\"code\":\"invalid_profile\",\"error\":\"name: must be at most 80 characters (got 81)\",\"field\":\"name\"}\n"),
		put("name control character", rb("A\u0007B", ""), "{\"code\":\"invalid_profile\",\"error\":\"name: must not contain control characters\",\"field\":\"name\"}\n"),
		put("name U+202E", rb("A\u202eB", ""), "{\"code\":\"invalid_profile\",\"error\":\"name: must not contain control characters\",\"field\":\"name\"}\n"),
		put("name invalid UTF-8", "{\"name\":\"\xff\xfe\"}", "{\"code\":\"invalid_profile\",\"error\":\"name: must contain a letter\",\"field\":\"name\"}\n"),
		put("phone letters", rb("Bảo", "abc"), "{\"code\":\"invalid_profile\",\"error\":\"phone: may contain only digits, spaces, + - . ( )\",\"field\":\"phone\"}\n"),
		put("phone 6 digits", rb("Bảo", "123456"), "{\"code\":\"invalid_profile\",\"error\":\"phone: must have 7 to 15 digits\",\"field\":\"phone\"}\n"),
		put("phone 16 digits", rb("Bảo", "1234567890123456"), "{\"code\":\"invalid_profile\",\"error\":\"phone: must have 7 to 15 digits\",\"field\":\"phone\"}\n"),
		put("phone 21 runes", rb("Bảo", "123456789012345678901"), "{\"code\":\"invalid_profile\",\"error\":\"phone: must be at most 20 characters (got 21)\",\"field\":\"phone\"}\n"),
		put("name and phone both bad: name wins", rb("", "abc"), "{\"code\":\"invalid_profile\",\"error\":\"name: is required\",\"field\":\"name\"}\n"),
	)

	// PUT /drivers/{id}.
	putd := func(name string, body any, want string) gErr {
		return e("e put driver "+name, "PUT /drivers/driver-1", drv, dsub, body, 400, want)
	}
	rows = append(rows,
		putd("invalid JSON", `{`, "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"),
		putd("empty body", ``, "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"),
		putd("name empty", dv("name", ""), "{\"code\":\"invalid_profile\",\"error\":\"name: is required\",\"field\":\"name\"}\n"),
		putd("name only digits", dv("name", "42"), "{\"code\":\"invalid_profile\",\"error\":\"name: must contain a letter\",\"field\":\"name\"}\n"),
		putd("phone letters", dv("phone", "abc"), "{\"code\":\"invalid_profile\",\"error\":\"phone: may contain only digits, spaces, + - . ( )\",\"field\":\"phone\"}\n"),
		putd("vehicleType TRUCK", dv("vehicleType", "TRUCK"), "{\"code\":\"invalid_profile\",\"error\":\"vehicleType: must be CAR or BIKE\",\"field\":\"vehicleType\"}\n"),
		putd("plate with #", dv("plate", "51A#123"), "{\"code\":\"invalid_profile\",\"error\":\"plate: may contain only letters, digits, spaces, - and .\",\"field\":\"plate\"}\n"),
		putd("plate under 3 alphanumerics", dv("plate", "A-1"), "{\"code\":\"invalid_profile\",\"error\":\"plate: must have at least 3 letters or digits\",\"field\":\"plate\"}\n"),
		putd("model 51 runes", dv("model", runes(51)), "{\"code\":\"invalid_profile\",\"error\":\"model: must be at most 50 characters (got 51)\",\"field\":\"model\"}\n"),
		putd("color 31 runes", dv("color", runes(31)), "{\"code\":\"invalid_profile\",\"error\":\"color: must be at most 30 characters (got 31)\",\"field\":\"color\"}\n"),
		putd("name wins over phone and plate", map[string]any{"name": "", "phone": "abc", "plate": "#"}, "{\"code\":\"invalid_profile\",\"error\":\"name: is required\",\"field\":\"name\"}\n"),
		putd("phone wins over vehicle and plate", map[string]any{"name": "Bảo", "phone": "abc", "vehicleType": "TRUCK", "plate": "#"}, "{\"code\":\"invalid_profile\",\"error\":\"phone: may contain only digits, spaces, + - . ( )\",\"field\":\"phone\"}\n"),
		putd("vehicleType wins over plate", map[string]any{"name": "Bảo", "vehicleType": "TRUCK", "plate": "#"}, "{\"code\":\"invalid_profile\",\"error\":\"vehicleType: must be CAR or BIKE\",\"field\":\"vehicleType\"}\n"),
		putd("plate wins over model", map[string]any{"name": "Bảo", "plate": "#", "model": runes(51)}, "{\"code\":\"invalid_profile\",\"error\":\"plate: may contain only letters, digits, spaces, - and .\",\"field\":\"plate\"}\n"),
		putd("model wins over color", map[string]any{"name": "Bảo", "model": runes(51), "color": runes(31)}, "{\"code\":\"invalid_profile\",\"error\":\"model: must be at most 50 characters (got 51)\",\"field\":\"model\"}\n"),
	)
	incomplete := func(name string, body map[string]any, want string) gErr {
		return e("e put driver under review "+name, "PUT /drivers/driver-1", drv, dsub, body, 400, want).db(gPending)
	}
	rows = append(rows,
		incomplete("without vehicleType and plate", map[string]any{"name": "Bảo"}, "{\"code\":\"invalid_profile\",\"error\":\"an application under review needs: vehicleType, plate\",\"field\":\"vehicleType\"}\n"),
		incomplete("without plate", map[string]any{"name": "Bảo", "vehicleType": "CAR"}, "{\"code\":\"invalid_profile\",\"error\":\"an application under review needs: plate\",\"field\":\"plate\"}\n"),
		incomplete("without vehicleType", map[string]any{"name": "Bảo", "plate": "51A-123.45"}, "{\"code\":\"invalid_profile\",\"error\":\"an application under review needs: vehicleType\",\"field\":\"vehicleType\"}\n"),
	)

	// POST /drivers/{id}/application.
	rows = append(rows,
		e("e apply with no profile", "POST /drivers/driver-1/application", drv, dsub, nil, 422, "{\"code\":\"incomplete_profile\",\"error\":\"save your profile (name, vehicle type, plate) before applying\",\"field\":\"name\"}\n").db(nil),
		e("e apply with only a name", "POST /drivers/driver-1/application", drv, dsub, nil, 422, "{\"code\":\"incomplete_profile\",\"error\":\"fill in before applying: vehicleType, plate\",\"field\":\"vehicleType\"}\n").db(func(f *fixture) {
			wantStatus(f.t, "put", f.putDriver("driver-1", map[string]any{"name": "Bảo"}), 200)
		}),
		e("e apply without a plate", "POST /drivers/driver-1/application", drv, dsub, nil, 422, "{\"code\":\"incomplete_profile\",\"error\":\"fill in before applying: plate\",\"field\":\"plate\"}\n").db(func(f *fixture) {
			wantStatus(f.t, "put", f.putDriver("driver-1", map[string]any{"name": "Bảo", "vehicleType": "BIKE"}), 200)
		}),
		e("e apply when suspended", "POST /drivers/driver-1/application", drv, dsub, nil, 409, "{\"code\":\"suspended\",\"error\":\"your account is suspended; contact support\",\"status\":\"SUSPENDED\"}\n").db(gSuspended),
	)

	// Review.
	for _, action := range []string{"reject", "suspend"} {
		path := "POST /drivers/driver-1/" + action
		rows = append(rows,
			e("e "+action+" without a body", path, adm, asub, nil, 400, gReasonRequired),
			e("e "+action+" with {}", path, adm, asub, `{}`, 400, gReasonRequired),
			e("e "+action+" with a 501-rune reason", path, adm, asub, map[string]string{"reason": runes(501)}, 400, gReasonTooLong),
			e("e "+action+" with a newline in the reason", path, adm, asub, map[string]string{"reason": "a\nb"}, 400, gReasonControl),
		)
	}
	rows = append(rows,
		e("e approve with a 501-rune reason", "POST /drivers/driver-1/approve", adm, asub, map[string]string{"reason": runes(501)}, 400, "{\"code\":\"invalid_profile\",\"error\":\"reason: must be at most 500 characters (got 501)\",\"field\":\"reason\"}\n"),
		e("e review invalid JSON", "POST /drivers/driver-1/approve", adm, asub, `{`, 400, "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"),
		e("e review bad reason on a 129-byte id: the reason is refused first", "POST /drivers/"+long+"/reject", adm, asub, `{}`, 400, "{\"code\":\"invalid_profile\",\"error\":\"reason: is required\",\"field\":\"reason\"}\n"),
		e("e review 129-byte id", "POST /drivers/"+long+"/approve", adm, asub, nil, 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n"),
		e("e review 129-byte id with a good reason", "POST /drivers/"+long+"/reject", adm, asub, map[string]string{"reason": "no"}, 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n"),
		e("e review invalid UTF-8 id", "POST /drivers/%ff/approve", adm, asub, nil, 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n"),
		e("e review unknown driver", "POST /drivers/nobody/approve", adm, asub, nil, 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n").db(nil),
		e("e approve an APPROVED driver", "POST /drivers/driver-1/approve", adm, asub, nil, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is APPROVED cannot be APPROVED\",\"status\":\"APPROVED\"}\n").db(gApproved),
		e("e reject an APPROVED driver", "POST /drivers/driver-1/reject", adm, asub, map[string]string{"reason": "no"}, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is APPROVED cannot be REJECTED\",\"status\":\"APPROVED\"}\n").db(gApproved),
		e("e suspend a PENDING driver", "POST /drivers/driver-1/suspend", adm, asub, map[string]string{"reason": "no"}, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is PENDING cannot be SUSPENDED\",\"status\":\"PENDING\"}\n").db(gPending),
		e("e suspend a REJECTED driver", "POST /drivers/driver-1/suspend", adm, asub, map[string]string{"reason": "no"}, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is REJECTED cannot be SUSPENDED\",\"status\":\"REJECTED\"}\n").db(gRejected),
		e("e reject a SUSPENDED driver", "POST /drivers/driver-1/reject", adm, asub, map[string]string{"reason": "no"}, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is SUSPENDED cannot be REJECTED\",\"status\":\"SUSPENDED\"}\n").db(gSuspended),
		e("e approve a driver with only a saved profile", "POST /drivers/driver-1/approve", adm, asub, nil, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is NONE cannot be APPROVED\",\"status\":\"NONE\"}\n").db(gSaved),
		e("e approve a REJECTED driver", "POST /drivers/driver-1/approve", adm, asub, nil, 409, "{\"code\":\"invalid_transition\",\"error\":\"a driver who is REJECTED cannot be APPROVED\",\"status\":\"REJECTED\"}\n").db(gRejected),
	)

	// Public view: one 404 for every driver who is not APPROVED.
	pub := func(name string, status int, want string, setup func(*fixture)) gErr {
		return e("e public "+name, "GET /drivers/driver-1/public", rid, rsub, nil, status, want).db(setup)
	}
	rows = append(rows,
		pub("missing driver", 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n", nil),
		pub("PENDING driver", 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n", gPending),
		pub("SUSPENDED driver", 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n", gSuspended),
		pub("REJECTED driver", 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n", gRejected),
		pub("driver with a saved profile only", 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n", gSaved),
		pub("rate limited", 429, "{\"code\":\"rate_limited\",\"error\":\"too many lookups, slow down\",\"retryAfter\":60}\n", gApproved).limited(),
	)

	// Admin queue.
	list := func(query, want string) gErr {
		return e("e list ?"+query, "GET /driver-applications?"+query, adm, asub, nil, 400, want)
	}
	rows = append(rows,
		list("status=NONE", "{\"code\":\"invalid_request\",\"error\":\"status must be PENDING, APPROVED, REJECTED or SUSPENDED\"}\n"),
		list("status=pending", "{\"code\":\"invalid_request\",\"error\":\"status must be PENDING, APPROVED, REJECTED or SUSPENDED\"}\n"),
		list("status=ALL", "{\"code\":\"invalid_request\",\"error\":\"status must be PENDING, APPROVED, REJECTED or SUSPENDED\"}\n"),
		list("limit=0", "{\"code\":\"invalid_request\",\"error\":\"limit must be from 1 to 100\"}\n"),
		list("limit=101", "{\"code\":\"invalid_request\",\"error\":\"limit must be from 1 to 100\"}\n"),
		list("limit=x", "{\"code\":\"invalid_request\",\"error\":\"limit must be from 1 to 100\"}\n"),
		list("cursor=junk", "{\"code\":\"invalid_request\",\"error\":\"cursor must come from a previous nextCursor\"}\n"),
		list("cursor=-1.x", "{\"code\":\"invalid_request\",\"error\":\"cursor must come from a previous nextCursor\"}\n"),
		list("cursor=5.", "{\"code\":\"invalid_request\",\"error\":\"cursor must come from a previous nextCursor\"}\n"),
		list("cursor=5."+long, `{"code":"invalid_request","error":"cursor must come from a previous nextCursor"}`+"\n"),
		list("status=NONE&limit=0&cursor=junk", "{\"code\":\"invalid_request\",\"error\":\"status must be PENDING, APPROVED, REJECTED or SUSPENDED\"}\n"),
		list("limit=0&cursor=junk", "{\"code\":\"invalid_request\",\"error\":\"limit must be from 1 to 100\"}\n"),
	)

	// Resync.
	rows = append(rows,
		e("e resync invalid JSON", "POST /driver-applications/resync", adm, asub, `{`, 400, "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"),
		e("e resync 129-byte driverId", "POST /driver-applications/resync", adm, asub, map[string]string{"driverId": long}, 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n"),
		e("e resync unknown driver", "POST /driver-applications/resync", adm, asub, map[string]string{"driverId": "nobody"}, 404, "{\"code\":\"not_found\",\"error\":\"no such driver\"}\n").db(nil),
		e("e resync while another runs", "POST /driver-applications/resync", adm, asub, nil, 409, "{\"code\":\"resync_running\",\"error\":\"a resync is already running; try again when it has finished\"}\n").db(func(f *fixture) {
			ctx := context.Background()
			c, err := f.db.Acquire(ctx)
			if err != nil {
				f.t.Fatal(err)
			}
			if _, err := c.Exec(ctx, `SELECT pg_advisory_lock($1)`, postgres.ResyncLockKey); err != nil {
				f.t.Fatal(err)
			}
			f.t.Cleanup(func() {
				c.Exec(ctx, `SELECT pg_advisory_unlock($1)`, postgres.ResyncLockKey)
				c.Release()
			})
		}),
	)

	// A closed pool: 500 with the generic body; what is logged is asserted in TestLogLinesAreStable.
	for _, r := range gInternalRows() {
		r.want = gInternal
		rows = append(rows, r)
	}

	offline := gOffline(t)
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			gCapture(t)
			f := offline
			if row.real {
				f = gFix(t)
				if row.setup != nil {
					row.setup(f)
				}
				if row.limiter {
					f.useLimiter(auth.NewLimiter(1.0/60, 3))
					for i := 0; i < 3; i++ {
						wantStatus(t, "burst", f.call(row.method, row.path, row.role, row.sub, row.body), 200)
					}
				}
			}
			res := f.call(row.method, row.path, row.role, row.sub, row.body)
			if res.status != row.status {
				t.Errorf("status %d, want %d (%q)", res.status, row.status, res.raw)
			}
			gEq(t, row.name+" Content-Type", res.header.Get("Content-Type"), gJSON)
			gEq(t, row.name+" Cache-Control", res.header.Get("Cache-Control"), row.cache)
			gEq(t, row.name+" WWW-Authenticate", res.header.Get("WWW-Authenticate"), row.wwwAuth)
			gEq(t, row.name, res.raw, row.want)
			if row.limiter {
				gEq(t, row.name+" Retry-After", res.header.Get("Retry-After"), "60")
			}
		})
	}
}

// gInternalRows are requests that reach the database and so answer 500 on a closed pool.
func gInternalRows() []gErr {
	rid, drv, adm := auth.Rider, auth.Driver, auth.Admin
	return []gErr{
		e("e 500 GET /riders/rider-1", "GET /riders/rider-1", rid, "rider-1", nil, 500, ""),
		e("e 500 PUT /riders/rider-1", "PUT /riders/rider-1", rid, "rider-1", map[string]string{"name": "Bảo"}, 500, ""),
		e("e 500 GET /drivers/driver-1", "GET /drivers/driver-1", drv, "driver-1", nil, 500, ""),
		e("e 500 PUT /drivers/driver-1", "PUT /drivers/driver-1", drv, "driver-1", vehicle("51A-123.45"), 500, ""),
		e("e 500 POST /drivers/driver-1/application", "POST /drivers/driver-1/application", drv, "driver-1", nil, 500, ""),
		e("e 500 GET /drivers/x/public", "GET /drivers/x/public", rid, "rider-1", nil, 500, ""),
		e("e 500 GET /driver-applications", "GET /driver-applications", adm, "ops-1", nil, 500, ""),
		e("e 500 POST /driver-applications/resync", "POST /driver-applications/resync", adm, "ops-1", nil, 500, ""),
		e("e 500 POST /drivers/driver-1/approve", "POST /drivers/driver-1/approve", adm, "ops-1", nil, 500, ""),
		e("e 500 POST /drivers/driver-1/reject", "POST /drivers/driver-1/reject", adm, "ops-1", map[string]string{"reason": "no"}, 500, ""),
		e("e 500 POST /drivers/driver-1/suspend", "POST /drivers/driver-1/suspend", adm, "ops-1", map[string]string{"reason": "no"}, 500, ""),
	}
}

// ------------------------------------------------------------- success bodies ----

func TestSuccessBodiesAreStable(t *testing.T) {
	f := gFix(t)
	step := func(name, req string, role auth.Role, sub string, body any, status int, want string) {
		t.Helper()
		m, p, _ := strings.Cut(req, " ")
		res := f.call(m, p, role, sub, body)
		if res.status != status {
			t.Errorf("%s: status %d, want %d (%q)", name, res.status, status, res.raw)
		}
		gEq(t, name+" Content-Type", res.header.Get("Content-Type"), gJSON)
		gEq(t, name+" Cache-Control", res.header.Get("Cache-Control"), gNoStore)
		gEq(t, name, res.raw, want)
		// Times are UTC (the Postgres adapter converts), whatever the session's zone.
		if regexp.MustCompile(`T[0-9:.]+[+-][0-9]{2}:[0-9]{2}"`).MatchString(res.raw) {
			t.Errorf("%s: a time with a numeric offset, want Z: %s", name, res.raw)
		}
	}
	tick := func() { f.now = f.now.Add(time.Minute) }
	rd, drv, adm := auth.Rider, auth.Driver, auth.Admin
	complete := vehicle("51A-123.45")

	// Riders.
	step("s rider with no profile", "GET /riders/rider-1", rd, "rider-1", nil, 200, "{\"riderId\":\"rider-1\",\"name\":\"\",\"phone\":\"\",\"updatedAt\":null}\n")
	step("s rider put", "PUT /riders/rider-1", rd, "rider-1", map[string]string{"name": "  Hoàng  Gia Bảo ", "phone": "+84 901 234 567"}, 200, "{\"riderId\":\"rider-1\",\"name\":\"Hoàng Gia Bảo\",\"phone\":\"+84 901 234 567\",\"updatedAt\":\"2026-03-04T05:06:07Z\"}\n")
	tick()
	step("s rider get after put", "GET /riders/rider-1", rd, "rider-1", nil, 200, "{\"riderId\":\"rider-1\",\"name\":\"Hoàng Gia Bảo\",\"phone\":\"+84 901 234 567\",\"updatedAt\":\"2026-03-04T05:06:07Z\"}\n")
	step("s rider put without a phone", "PUT /riders/rider-1", rd, "rider-1", map[string]string{"name": "Bảo", "unknown": "ignored"}, 200, "{\"riderId\":\"rider-1\",\"name\":\"Bảo\",\"phone\":\"\",\"updatedAt\":\"2026-03-04T05:07:07Z\"}\n")

	// A driver from nothing to approved.
	step("s driver with no row", "GET /drivers/driver-1", drv, "driver-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"\",\"phone\":\"\",\"vehicleType\":\"\",\"plate\":\"\",\"model\":\"\",\"color\":\"\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":null,\"updatedAt\":null}\n")
	step("s driver put", "PUT /drivers/driver-1", drv, "driver-1", complete, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:07:07Z\",\"updatedAt\":\"2026-03-04T05:07:07Z\"}\n")
	step("s driver put lowercase vehicleType and a padded plate", "PUT /drivers/driver-1", drv, "driver-1",
		map[string]any{"name": "Nguyễn Văn An", "phone": "0901 234 567", "vehicleType": " car ", "plate": " 51a-123.45 ", "model": "Toyota Vios", "color": "Trắng", "status": "APPROVED"}, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:07:07Z\",\"updatedAt\":\"2026-03-04T05:07:07Z\"}\n")
	tick()
	step("s driver get after save", "GET /drivers/driver-1", drv, "driver-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:07:07Z\",\"updatedAt\":\"2026-03-04T05:07:07Z\"}\n")
	tick()
	step("s driver apply", "POST /drivers/driver-1/application", drv, "driver-1", nil, 201, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"PENDING\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:09:07Z\",\"updatedAt\":\"2026-03-04T05:09:07Z\"}\n")
	tick()
	step("s driver apply again", "POST /drivers/driver-1/application", drv, "driver-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"PENDING\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:09:07Z\",\"updatedAt\":\"2026-03-04T05:09:07Z\"}\n")
	tick()
	step("s approve with an empty body", "POST /drivers/driver-1/approve", adm, "ops-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"APPROVED\",\"reason\":\"\",\"reviewedAt\":\"2026-03-04T05:11:07Z\",\"statusAt\":\"2026-03-04T05:11:07Z\",\"updatedAt\":\"2026-03-04T05:11:07Z\",\"reviewedBy\":\"ops-1\"}\n")
	step("s driver get after approve", "GET /drivers/driver-1", drv, "driver-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"APPROVED\",\"reason\":\"\",\"reviewedAt\":\"2026-03-04T05:11:07Z\",\"statusAt\":\"2026-03-04T05:11:07Z\",\"updatedAt\":\"2026-03-04T05:11:07Z\"}\n")
	step("s public view", "GET /drivers/driver-1/public", rd, "rider-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\"}\n")
	step("s public view for an admin", "GET /drivers/driver-1/public", adm, "ops-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\"}\n")
	tick()
	step("s driver put that changes the vehicle", "PUT /drivers/driver-1", drv, "driver-1", vehicle("30G-246.80"), 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"PENDING\",\"reason\":\"You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again.\",\"reviewedAt\":\"2026-03-04T05:12:07Z\",\"statusAt\":\"2026-03-04T05:12:07Z\",\"updatedAt\":\"2026-03-04T05:12:07Z\"}\n")
	step("s driver put that only describes it", "PUT /drivers/driver-1", drv, "driver-1", map[string]any{"name": "Nguyễn Văn An", "vehicleType": "CAR", "plate": "30G-246.80", "model": "Kia", "color": "Đỏ"}, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Kia\",\"color\":\"Đỏ\",\"status\":\"PENDING\",\"reason\":\"You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again.\",\"reviewedAt\":\"2026-03-04T05:12:07Z\",\"statusAt\":\"2026-03-04T05:12:07Z\",\"updatedAt\":\"2026-03-04T05:12:07Z\"}\n")

	// The queue: two applications, an empty one, a page with a cursor.
	tick()
	step("s second driver put", "PUT /drivers/driver-2", drv, "driver-2", map[string]any{"name": "Trần Thị Bích", "phone": "0900 000 002", "vehicleType": "BIKE", "plate": "59-X1 123.45"}, 200, "{\"driverId\":\"driver-2\",\"name\":\"Trần Thị Bích\",\"phone\":\"0900 000 002\",\"vehicleType\":\"BIKE\",\"plate\":\"59-X1 123.45\",\"model\":\"\",\"color\":\"\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:13:07Z\",\"updatedAt\":\"2026-03-04T05:13:07Z\"}\n")
	tick()
	step("s second driver apply", "POST /drivers/driver-2/application", drv, "driver-2", nil, 201, "{\"driverId\":\"driver-2\",\"name\":\"Trần Thị Bích\",\"phone\":\"0900 000 002\",\"vehicleType\":\"BIKE\",\"plate\":\"59-X1 123.45\",\"model\":\"\",\"color\":\"\",\"status\":\"PENDING\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:14:07Z\",\"updatedAt\":\"2026-03-04T05:14:07Z\"}\n")
	step("s queue with two applications", "GET /driver-applications", adm, "ops-1", nil, 200, "{\"applications\":[{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Kia\",\"color\":\"Đỏ\",\"status\":\"PENDING\",\"reason\":\"You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again.\",\"reviewedAt\":\"2026-03-04T05:12:07Z\",\"statusAt\":\"2026-03-04T05:12:07Z\",\"updatedAt\":\"2026-03-04T05:12:07Z\",\"reviewedBy\":\"system\"},{\"driverId\":\"driver-2\",\"name\":\"Trần Thị Bích\",\"phone\":\"0900 000 002\",\"vehicleType\":\"BIKE\",\"plate\":\"59-X1 123.45\",\"model\":\"\",\"color\":\"\",\"status\":\"PENDING\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:14:07Z\",\"updatedAt\":\"2026-03-04T05:14:07Z\",\"reviewedBy\":\"\"}],\"nextCursor\":null}\n")
	step("s empty queue", "GET /driver-applications?status=REJECTED", adm, "ops-1", nil, 200, `{"applications":[],"nextCursor":null}`+"\n")
	step("s queue page with a cursor", "GET /driver-applications?limit=1", adm, "ops-1", nil, 200, "{\"applications\":[{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Kia\",\"color\":\"Đỏ\",\"status\":\"PENDING\",\"reason\":\"You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again.\",\"reviewedAt\":\"2026-03-04T05:12:07Z\",\"statusAt\":\"2026-03-04T05:12:07Z\",\"updatedAt\":\"2026-03-04T05:12:07Z\",\"reviewedBy\":\"system\"}],\"nextCursor\":\"1772601127000000.driver-1\"}\n")
	step("s queue last page", "GET /driver-applications?limit=1&cursor=1772601127000000.driver-1", adm, "ops-1", nil, 200, "{\"applications\":[{\"driverId\":\"driver-2\",\"name\":\"Trần Thị Bích\",\"phone\":\"0900 000 002\",\"vehicleType\":\"BIKE\",\"plate\":\"59-X1 123.45\",\"model\":\"\",\"color\":\"\",\"status\":\"PENDING\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:14:07Z\",\"updatedAt\":\"2026-03-04T05:14:07Z\",\"reviewedBy\":\"\"}],\"nextCursor\":null}\n")

	// Decisions.
	tick()
	step("s reject", "POST /drivers/driver-2/reject", adm, "ops-1", map[string]string{"reason": "  documents   unreadable "}, 200, "{\"driverId\":\"driver-2\",\"name\":\"Trần Thị Bích\",\"phone\":\"0900 000 002\",\"vehicleType\":\"BIKE\",\"plate\":\"59-X1 123.45\",\"model\":\"\",\"color\":\"\",\"status\":\"REJECTED\",\"reason\":\"documents unreadable\",\"reviewedAt\":\"2026-03-04T05:15:07Z\",\"statusAt\":\"2026-03-04T05:15:07Z\",\"updatedAt\":\"2026-03-04T05:15:07Z\",\"reviewedBy\":\"ops-1\"}\n")
	tick()
	step("s approve after the vehicle change", "POST /drivers/driver-1/approve", adm, "ops-2", map[string]string{"reason": "plate checked"}, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Kia\",\"color\":\"Đỏ\",\"status\":\"APPROVED\",\"reason\":\"plate checked\",\"reviewedAt\":\"2026-03-04T05:16:07Z\",\"statusAt\":\"2026-03-04T05:16:07Z\",\"updatedAt\":\"2026-03-04T05:16:07Z\",\"reviewedBy\":\"ops-2\"}\n")
	tick()
	step("s suspend", "POST /drivers/driver-1/suspend", adm, "ops-1", map[string]string{"reason": "complaints"}, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Kia\",\"color\":\"Đỏ\",\"status\":\"SUSPENDED\",\"reason\":\"complaints\",\"reviewedAt\":\"2026-03-04T05:17:07Z\",\"statusAt\":\"2026-03-04T05:17:07Z\",\"updatedAt\":\"2026-03-04T05:17:07Z\",\"reviewedBy\":\"ops-1\"}\n")
	tick()
	step("s approve a suspended driver", "POST /drivers/driver-1/approve", adm, "ops-1", nil, 200, "{\"driverId\":\"driver-1\",\"name\":\"Nguyễn Văn An\",\"phone\":\"\",\"vehicleType\":\"CAR\",\"plate\":\"30G-246.80\",\"model\":\"Kia\",\"color\":\"Đỏ\",\"status\":\"APPROVED\",\"reason\":\"\",\"reviewedAt\":\"2026-03-04T05:18:07Z\",\"statusAt\":\"2026-03-04T05:18:07Z\",\"updatedAt\":\"2026-03-04T05:18:07Z\",\"reviewedBy\":\"ops-1\"}\n")
	step("s queue of the suspended and the rejected", "GET /driver-applications?status=REJECTED&limit=100", adm, "ops-1", nil, 200, "{\"applications\":[{\"driverId\":\"driver-2\",\"name\":\"Trần Thị Bích\",\"phone\":\"0900 000 002\",\"vehicleType\":\"BIKE\",\"plate\":\"59-X1 123.45\",\"model\":\"\",\"color\":\"\",\"status\":\"REJECTED\",\"reason\":\"documents unreadable\",\"reviewedAt\":\"2026-03-04T05:15:07Z\",\"statusAt\":\"2026-03-04T05:15:07Z\",\"updatedAt\":\"2026-03-04T05:15:07Z\",\"reviewedBy\":\"ops-1\"}],\"nextCursor\":null}\n")

	// Resync: the body of the counts.
	step("s third driver put", "PUT /drivers/driver-3", drv, "driver-3", complete, 200, "{\"driverId\":\"driver-3\",\"name\":\"Nguyễn Văn An\",\"phone\":\"0901 234 567\",\"vehicleType\":\"CAR\",\"plate\":\"51A-123.45\",\"model\":\"Toyota Vios\",\"color\":\"Trắng\",\"status\":\"NONE\",\"reason\":\"\",\"reviewedAt\":null,\"statusAt\":\"2026-03-04T05:18:07Z\",\"updatedAt\":\"2026-03-04T05:18:07Z\"}\n")
	tick()
	step("s resync all", "POST /driver-applications/resync", adm, "ops-1", nil, 200, "{\"counts\":{\"APPROVED\":1,\"REJECTED\":1,\"SUSPENDED\":0},\"queued\":2,\"skipped\":0}\n")
	step("s resync one", "POST /driver-applications/resync", adm, "ops-1", map[string]string{"driverId": "driver-2"}, 200, "{\"counts\":{\"APPROVED\":0,\"REJECTED\":1,\"SUSPENDED\":0},\"queued\":1,\"skipped\":0}\n")
	step("s resync one that is not resyncable", "POST /driver-applications/resync", adm, "ops-1", map[string]string{"driverId": "driver-3"}, 200, "{\"counts\":{\"APPROVED\":0,\"REJECTED\":0,\"SUSPENDED\":0},\"queued\":0,\"skipped\":1}\n")
}

// ------------------------------------------------------------------ log lines ----

func TestLogLinesAreStable(t *testing.T) {
	buf := gCapture(t)
	f := gFix(t)
	rd, drv, adm := auth.Rider, auth.Driver, auth.Admin
	put := func(id string, body any) func() { return func() { f.putDriver(id, body.(map[string]any)) } }
	decide := func(action, id, reason string) func() { return func() { f.decide(action, id, reason) } }

	steps := []struct {
		name string
		do   func()
		want string
	}{
		{"l rider updated", func() { f.call("PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": "Bảo"}) }, "rider rider-1 updated their profile\n"},
		{"l rider read logs nothing", func() { f.call("GET", "/riders/rider-1", rd, "rider-1", nil) }, ""},
		{"l rider refused logs nothing", func() { f.call("PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": ""}) }, ""},
		{"l driver updated", put("driver-1", vehicle("51A-123.45")), "driver driver-1 updated their profile\n"},
		{"l driver refused logs nothing", put("driver-1", dv("plate", "#")), ""},
		{"l applied", func() { f.apply("driver-1") }, "driver driver-1 applied: PENDING\n"},
		{"l replayed apply logs nothing", func() { f.apply("driver-1") }, ""},
		{"l approve", decide("approve", "driver-1", ""), "driver driver-1: approve by ops-1 -> APPROVED\n"},
		{"l vehicle changed", put("driver-1", vehicle("30G-246.80")), "driver driver-1 changed vehicle: back to PENDING, out of service until approved\n"},
		{"l driver updated, vehicle only described", put("driver-1", dv("model", "Kia")), "driver driver-1 updated their profile\n"},
		{"l approve after vehicle change", decide("approve", "driver-1", "ok"), "driver driver-1: approve by ops-1 -> APPROVED\n"},
		{"l suspend", decide("suspend", "driver-1", "complaints"), "driver driver-1: suspend by ops-1 -> SUSPENDED\n"},
		{"l reject", func() { f.applied("driver-2"); buf.Reset(); f.decide("reject", "driver-2", "no") }, "driver driver-2: reject by ops-1 -> REJECTED\n"},
		{"l refused review logs nothing", decide("approve", "driver-2", ""), ""},
		{"l unknown driver review logs nothing", decide("approve", "nobody", ""), ""},
		{"l read paths log nothing", func() {
			f.call("GET", "/drivers/driver-1", drv, "driver-1", nil)
			f.call("GET", "/drivers/driver-1/public", rd, "rider-1", nil)
			f.call("GET", "/drivers/nobody/public", rd, "rider-1", nil)
			f.call("GET", "/driver-applications", adm, "ops-1", nil)
		}, ""},
		{"l resync all", func() { f.resync(nil) }, "resync by ops-1: all drivers: 2 event(s) queued (APPROVED 0, REJECTED 1, SUSPENDED 1, 0 skipped)\n"},
		{"l resync one", func() { f.resync(map[string]string{"driverId": "driver-2"}) }, "resync by ops-1: driver driver-2: 1 event(s) queued (APPROVED 0, REJECTED 1, SUSPENDED 0, 0 skipped)\n"},
		{"l resync one that is skipped", func() { f.applied("driver-3"); buf.Reset(); f.resync(map[string]string{"driverId": "driver-3"}) }, "resync by ops-1: driver driver-3: 0 event(s) queued (APPROVED 0, REJECTED 0, SUSPENDED 0, 1 skipped)\n"},
		{"l resync unknown driver logs nothing", func() { f.resync(map[string]string{"driverId": "nobody"}) }, ""},
		{"l resync refused invalid JSON logs nothing", func() { f.resync(`{`) }, ""},
		{"l resync refused while another runs logs nothing", func() {
			ctx := context.Background()
			c, err := f.db.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Release()
			if _, err := c.Exec(ctx, `SELECT pg_advisory_lock($1)`, postgres.ResyncLockKey); err != nil {
				t.Fatal(err)
			}
			f.resync(nil)
			c.Exec(ctx, `SELECT pg_advisory_unlock($1)`, postgres.ResyncLockKey)
		}, ""},
		{"l resync that fails in the encoder logs only the 500", func() { f.useEncoder(failingEncoder{}); f.resync(nil); f.useEncoder(jsonEncoder{}) }, "resync: encoding the review result: schema registry down\n"},
		{"l approve that fails in the encoder logs only the 500", func() {
			f.useEncoder(failingEncoder{})
			f.decide("approve", "driver-3", "")
			f.useEncoder(jsonEncoder{})
		}, "approve driver driver-3: encoding the review result: schema registry down\n"},
	}
	for _, s := range steps {
		buf.Reset()
		s.do()
		gEq(t, s.name, buf.String(), s.want)
	}

	// The 500 lines, on a closed pool.
	off := gOffline(t)
	wantLines := map[string]string{
		"GET /riders/rider-1":                "load rider rider-1: closed pool\n",
		"PUT /riders/rider-1":                "save rider rider-1: closed pool\n",
		"GET /drivers/driver-1":              "load driver driver-1: closed pool\n",
		"PUT /drivers/driver-1":              "save driver driver-1: closed pool\n",
		"POST /drivers/driver-1/application": "apply driver driver-1: closed pool\n",
		"GET /drivers/x/public":              "load public driver: closed pool\n",
		"GET /driver-applications":           "list driver applications: closed pool\n",
		"POST /driver-applications/resync":   "resync: closed pool\n",
		"POST /drivers/driver-1/approve":     "approve driver driver-1: closed pool\n",
		"POST /drivers/driver-1/reject":      "reject driver driver-1: closed pool\n",
		"POST /drivers/driver-1/suspend":     "suspend driver driver-1: closed pool\n",
	}
	rows500 := gInternalRows()
	if len(rows500) != len(wantLines) {
		t.Fatalf("%d internal-error rows, %d expected lines", len(rows500), len(wantLines))
	}
	for _, r := range rows500 {
		buf.Reset()
		off.call(r.method, r.path, r.role, r.sub, r.body)
		gEq(t, "l 500 "+r.method+" "+r.path, buf.String(), wantLines[r.method+" "+r.path])
	}
	buf.Reset()
	off.call("GET", "/health", "", "", nil)
	gEq(t, "l health logs nothing", buf.String(), "")
}

// -------------------------------------------------------------------- counters ----

var gMetricNames = map[string]bool{
	"profile_requests_rejected_total": true,
	"profile_updates_total":           true,
	"profile_applications_total":      true,
	"profile_reviews_total":           true,
	"profile_public_lookups_total":    true,
	"profile_resync_runs_total":       true,
	"profile_resync_events_total":     true,
}

// gCounters reads the seven counters through the default registry, as /metrics does.
func gCounters(t *testing.T) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if !gMetricNames[mf.GetName()] {
			continue
		}
		for _, m := range mf.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			sort.Strings(labels)
			out[mf.GetName()+"{"+strings.Join(labels, ",")+"}"] = m.GetCounter().GetValue()
		}
	}
	return out
}

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

func TestCountersMoveAsRecorded(t *testing.T) {
	long := strings.Repeat("x", 129)
	rd, drv, adm := auth.Rider, auth.Driver, auth.Admin
	rows := []struct {
		name    string
		offline bool
		setup   func(*fixture)
		do      func(*fixture)
		want    string
	}{
		// Refusals, one per code.
		{"c invalid_request", false, nil, func(f *fixture) { f.call("PUT", "/riders/rider-1", rd, "rider-1", `{`) }, "profile_requests_rejected_total{reason=\"invalid_request\"}=1"},
		{"c invalid_request on the list", false, nil, func(f *fixture) { f.call("GET", "/driver-applications?limit=0", adm, "ops-1", nil) }, "profile_requests_rejected_total{reason=\"invalid_request\"}=1"},
		{"c invalid_profile", false, nil, func(f *fixture) { f.call("PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": ""}) }, "profile_requests_rejected_total{reason=\"invalid_profile\"}=1"},
		{"c invalid_profile on a review", false, nil, func(f *fixture) { f.decide("reject", "driver-1", "") }, "profile_requests_rejected_total{reason=\"invalid_profile\"}=1"},
		{"c invalid_profile from an incomplete driver", false, gPending, func(f *fixture) { f.putDriver("driver-1", map[string]any{"name": "Bảo"}) }, "profile_requests_rejected_total{reason=\"invalid_profile\"}=1"},
		{"c forbidden", false, nil, func(f *fixture) { f.call("GET", "/riders/rider-2", rd, "rider-1", nil) }, "profile_requests_rejected_total{reason=\"forbidden\"}=1"},
		{"c token verifier refusals are not counted", false, nil, func(f *fixture) {
			f.call("GET", "/riders/rider-1", "", "", nil)
			f.call("GET", "/driver-applications", rd, "rider-1", nil)
		}, ""},
		{"c not_found on a review", false, nil, func(f *fixture) { f.decide("approve", "nobody", "") }, "profile_requests_rejected_total{reason=\"not_found\"}=1"},
		{"c not_found on a long review id", false, nil, func(f *fixture) { f.call("POST", "/drivers/"+long+"/approve", adm, "ops-1", nil) }, "profile_requests_rejected_total{reason=\"not_found\"}=1"},
		{"c incomplete_profile", false, nil, func(f *fixture) { f.apply("driver-1") }, "profile_requests_rejected_total{reason=\"incomplete_profile\"}=1"},
		{"c incomplete_profile with a partial profile", false, func(f *fixture) {
			f.putDriver("driver-1", map[string]any{"name": "Bảo"})
		}, func(f *fixture) { f.apply("driver-1") }, "profile_requests_rejected_total{reason=\"incomplete_profile\"}=1"},
		{"c suspended", false, gSuspended, func(f *fixture) { f.apply("driver-1") }, "profile_requests_rejected_total{reason=\"suspended\"}=1"},
		{"c invalid_transition", false, gApproved, func(f *fixture) { f.decide("approve", "driver-1", "") }, "profile_requests_rejected_total{reason=\"invalid_transition\"}=1"},
		{"c rate_limited", false, func(f *fixture) {
			gApproved(f)
			f.useLimiter(auth.NewLimiter(1.0/60, 1))
			f.call("GET", "/drivers/driver-1/public", rd, "rider-1", nil)
		}, func(f *fixture) { f.call("GET", "/drivers/driver-1/public", rd, "rider-1", nil) }, "profile_requests_rejected_total{reason=\"rate_limited\"}=1"},
		{"c resync_running", false, func(f *fixture) {
			ctx := context.Background()
			c, err := f.db.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Exec(ctx, `SELECT pg_advisory_lock($1)`, postgres.ResyncLockKey); err != nil {
				t.Fatal(err)
			}
			f.t.Cleanup(func() { c.Exec(ctx, `SELECT pg_advisory_unlock($1)`, postgres.ResyncLockKey); c.Release() })
		}, func(f *fixture) { f.resync(nil) }, "profile_requests_rejected_total{reason=\"resync_running\"}=1 profile_resync_runs_total{result=\"refused_running\"}=1"},
		{"c an internal error counts no refusal", true, nil, func(f *fixture) {
			f.call("GET", "/riders/rider-1", rd, "rider-1", nil)
			f.call("PUT", "/drivers/driver-1", drv, "driver-1", vehicle("51A-123.45"))
			f.call("GET", "/drivers/x/public", rd, "rider-1", nil)
			f.call("GET", "/driver-applications", adm, "ops-1", nil)
		}, ""},
		{"c an internal error in resync counts a failed run", true, nil, func(f *fixture) { f.resync(nil) }, "profile_resync_runs_total{result=\"failed\"}=1"},

		// Successes.
		{"c rider update", false, nil, func(f *fixture) { f.call("PUT", "/riders/rider-1", rd, "rider-1", map[string]string{"name": "Bảo"}) }, "profile_updates_total{kind=\"rider\"}=1"},
		{"c rider read", false, nil, func(f *fixture) { f.call("GET", "/riders/rider-1", rd, "rider-1", nil) }, ""},
		{"c driver update", false, nil, func(f *fixture) { f.putDriver("driver-1", vehicle("51A-123.45")) }, "profile_updates_total{kind=\"driver\"}=1"},
		{"c driver update that changes the vehicle", false, gApproved, func(f *fixture) { f.putDriver("driver-1", vehicle("30G-246.80")) }, "profile_reviews_total{decision=\"vehicle_changed\"}=1 profile_updates_total{kind=\"driver\"}=1"},
		{"c driver update that only describes the vehicle", false, gApproved, func(f *fixture) { f.putDriver("driver-1", dv("color", "Đỏ")) }, "profile_updates_total{kind=\"driver\"}=1"},
		{"c application submitted", false, gSaved, func(f *fixture) { f.apply("driver-1") }, "profile_applications_total{result=\"submitted\"}=1"},
		{"c application replayed while PENDING", false, gPending, func(f *fixture) { f.apply("driver-1") }, "profile_applications_total{result=\"replayed\"}=1"},
		{"c application replayed while APPROVED", false, gApproved, func(f *fixture) { f.apply("driver-1") }, "profile_applications_total{result=\"replayed\"}=1"},
		{"c application submitted again after a rejection", false, gRejected, func(f *fixture) { f.apply("driver-1") }, "profile_applications_total{result=\"submitted\"}=1"},
		{"c approve", false, gPending, func(f *fixture) { f.decide("approve", "driver-1", "") }, "profile_reviews_total{decision=\"approved\"}=1"},
		{"c approve a suspended driver", false, gSuspended, func(f *fixture) { f.decide("approve", "driver-1", "") }, "profile_reviews_total{decision=\"approved\"}=1"},
		{"c reject", false, gPending, func(f *fixture) { f.decide("reject", "driver-1", "no") }, "profile_reviews_total{decision=\"rejected\"}=1"},
		{"c suspend", false, gApproved, func(f *fixture) { f.decide("suspend", "driver-1", "no") }, "profile_reviews_total{decision=\"suspended\"}=1"},
		{"c a review whose event cannot be queued counts nothing", false, gPending, func(f *fixture) {
			f.useEncoder(failingEncoder{})
			f.decide("approve", "driver-1", "")
		}, ""},
		{"c public found", false, gApproved, func(f *fixture) { f.call("GET", "/drivers/driver-1/public", rd, "rider-1", nil) }, "profile_public_lookups_total{result=\"found\"}=1"},
		{"c public not_found for a missing driver", false, nil, func(f *fixture) { f.call("GET", "/drivers/driver-1/public", rd, "rider-1", nil) }, "profile_public_lookups_total{result=\"not_found\"}=1 profile_requests_rejected_total{reason=\"not_found\"}=1"},
		{"c public not_found for a PENDING driver", false, gPending, func(f *fixture) { f.call("GET", "/drivers/driver-1/public", rd, "rider-1", nil) }, "profile_public_lookups_total{result=\"not_found\"}=1 profile_requests_rejected_total{reason=\"not_found\"}=1"},
		{"c list", false, gPending, func(f *fixture) { f.call("GET", "/driver-applications", adm, "ops-1", nil) }, ""},
		{"c resync all", false, func(f *fixture) {
			f.approved("driver-a")
			f.approved("driver-b")
			f.applied("driver-r")
			f.decide("reject", "driver-r", "no")
			f.approved("driver-s")
			f.decide("suspend", "driver-s", "no")
			f.applied("driver-p")
		}, func(f *fixture) { f.resync(nil) }, "profile_resync_events_total{status=\"APPROVED\"}=2 profile_resync_events_total{status=\"REJECTED\"}=1 profile_resync_events_total{status=\"SUSPENDED\"}=1 profile_resync_runs_total{result=\"completed\"}=1"},
		{"c resync of nothing", false, nil, func(f *fixture) { f.resync(nil) }, "profile_resync_runs_total{result=\"completed\"}=1"},
		{"c resync one", false, gApproved, func(f *fixture) { f.resync(map[string]string{"driverId": "driver-1"}) }, "profile_resync_events_total{status=\"APPROVED\"}=1 profile_resync_runs_total{result=\"completed\"}=1"},
		{"c resync one that is PENDING is skipped", false, gPending, func(f *fixture) { f.resync(map[string]string{"driverId": "driver-1"}) }, "profile_resync_runs_total{result=\"completed\"}=1"},
		{"c resync unknown driver", false, nil, func(f *fixture) { f.resync(map[string]string{"driverId": "nobody"}) }, "profile_requests_rejected_total{reason=\"not_found\"}=1"},
		{"c resync long driverId", false, nil, func(f *fixture) { f.resync(map[string]string{"driverId": long}) }, "profile_requests_rejected_total{reason=\"not_found\"}=1"},
		{"c resync that fails in the encoder", false, gApproved, func(f *fixture) { f.useEncoder(failingEncoder{}); f.resync(nil) }, "profile_resync_runs_total{result=\"failed\"}=1"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			gCapture(t)
			var f *fixture
			if row.offline {
				f = gOffline(t)
			} else {
				f = gFix(t)
			}
			if row.setup != nil {
				row.setup(f)
			}
			before := gCounters(t)
			row.do(f)
			gEq(t, row.name, gDelta(before, gCounters(t)), row.want)
		})
	}
}

// --------------------------------------------------------------------- outbox ----

type gEvent struct {
	topic, key string
	payload    map[string]any
}

// gQueued is every outbox row of this service from index n on, decoded.
func gQueued(t *testing.T, f *fixture, from int) []gEvent {
	t.Helper()
	rows, err := f.db.Query(context.Background(), `SELECT topic, key, payload FROM outbox WHERE source = 'profile-service' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []gEvent
	for rows.Next() {
		var e gEvent
		var key, payload []byte
		if err := rows.Scan(&e.topic, &key, &payload); err != nil {
			t.Fatal(err)
		}
		e.key = string(key)
		if err := json.Unmarshal(payload, &e.payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	if from > len(out) {
		from = len(out)
	}
	return out[from:]
}

// gSummary renders events as "topic key {fields}" lines; eventId must be a UUID and is not
// part of the line (it is random).
func gSummary(t *testing.T, evs []gEvent) string {
	t.Helper()
	var b strings.Builder
	for _, ev := range evs {
		id, _ := ev.payload["eventId"].(string)
		if _, err := uuid.Parse(id); err != nil {
			t.Errorf("eventId %q is not a UUID", id)
		}
		var keys []string
		for k := range ev.payload {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "%s key=%s fields=%s", ev.topic, ev.key, strings.Join(keys, ","))
		for _, k := range []string{"driverId", "status", "vehicleType", "reason", "reviewedBy", "occurredAt"} {
			fmt.Fprintf(&b, " %s=%q", k, ev.payload[k])
		}
		b.WriteString("\n")
	}
	return b.String()
}

func TestOutboxPayloadsAreStable(t *testing.T) {
	later := func(f *fixture, d time.Duration) { f.now = f.now.Add(d) }
	rows := []struct {
		name  string
		setup func(*fixture)
		do    func(*fixture)
		// rowTime says the event's occurredAt must equal this driver's reviewed_at.
		rowTime string
		want    string
	}{
		{"o save with a vehicle change", gApproved, func(f *fixture) {
			later(f, time.Minute)
			f.putDriver("driver-1", vehicle("30G-246.80"))
		}, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"SUSPENDED\" vehicleType=\"CAR\" reason=\"You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again.\" reviewedBy=\"system\" occurredAt=\"2026-03-04T05:07:07Z\"\n"},
		{"o save without a vehicle change queues nothing", gApproved, func(f *fixture) {
			later(f, time.Minute)
			f.putDriver("driver-1", dv("model", "Kia"))
		}, "", ""},
		{"o save of a driver under review queues nothing", gPending, func(f *fixture) {
			f.putDriver("driver-1", vehicle("30G-246.80"))
		}, "", ""},
		{"o approve without a reason", gPending, func(f *fixture) {
			later(f, time.Minute)
			f.decide("approve", "driver-1", "")
		}, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"APPROVED\" vehicleType=\"CAR\" reason=\"\" reviewedBy=\"ops-1\" occurredAt=\"2026-03-04T05:07:07Z\"\n"},
		{"o approve with a reason", gPending, func(f *fixture) {
			later(f, time.Minute)
			f.decide("approve", "driver-1", "plate checked")
		}, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"APPROVED\" vehicleType=\"CAR\" reason=\"plate checked\" reviewedBy=\"ops-1\" occurredAt=\"2026-03-04T05:07:07Z\"\n"},
		{"o reject", gPending, func(f *fixture) {
			later(f, time.Minute)
			f.decide("reject", "driver-1", "documents unreadable")
		}, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"REJECTED\" vehicleType=\"CAR\" reason=\"documents unreadable\" reviewedBy=\"ops-1\" occurredAt=\"2026-03-04T05:07:07Z\"\n"},
		{"o suspend", gApproved, func(f *fixture) {
			later(f, time.Minute)
			f.decide("suspend", "driver-1", "complaints")
		}, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"SUSPENDED\" vehicleType=\"CAR\" reason=\"complaints\" reviewedBy=\"ops-1\" occurredAt=\"2026-03-04T05:07:07Z\"\n"},
		{"o a decision with the clock stepped back is dated after the last one", gApproved, func(f *fixture) {
			later(f, -time.Hour)
			f.decide("suspend", "driver-1", "complaints")
		}, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"SUSPENDED\" vehicleType=\"CAR\" reason=\"complaints\" reviewedBy=\"ops-1\" occurredAt=\"2026-03-04T05:06:07.000001Z\"\n"},
		{"o bike driver", func(f *fixture) {
			f.putDriver("driver-1", map[string]any{"name": "Bảo", "vehicleType": "BIKE", "plate": "59-X1 123.45"})
			f.apply("driver-1")
		}, func(f *fixture) { f.decide("approve", "driver-1", "") }, "driver-1", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"APPROVED\" vehicleType=\"BIKE\" reason=\"\" reviewedBy=\"ops-1\" occurredAt=\"2026-03-04T05:06:07Z\"\n"},
		{"o resync of every status: reason falls back to resync and reviewedBy is resync", func(f *fixture) {
			f.approved("driver-a")
			f.applied("driver-r")
			f.decide("reject", "driver-r", "documents unreadable")
			f.approved("driver-s")
			f.decide("suspend", "driver-s", "complaints")
			f.applied("driver-p")
			f.putDriver("driver-n", vehicle("30G-246.80"))
		}, func(f *fixture) {
			later(f, time.Hour)
			f.resync(nil)
		}, "", "driver.application.reviewed key=driver-a fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-a\" status=\"APPROVED\" vehicleType=\"CAR\" reason=\"resync\" reviewedBy=\"resync\" occurredAt=\"2026-03-04T06:06:07Z\"\ndriver.application.reviewed key=driver-r fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-r\" status=\"REJECTED\" vehicleType=\"CAR\" reason=\"documents unreadable\" reviewedBy=\"resync\" occurredAt=\"2026-03-04T06:06:07Z\"\ndriver.application.reviewed key=driver-s fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-s\" status=\"SUSPENDED\" vehicleType=\"CAR\" reason=\"complaints\" reviewedBy=\"resync\" occurredAt=\"2026-03-04T06:06:07Z\"\n"},
		{"o resync with the clock no later than the last decision", gApproved, func(f *fixture) {
			f.resync(nil)
		}, "", "driver.application.reviewed key=driver-1 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-1\" status=\"APPROVED\" vehicleType=\"CAR\" reason=\"resync\" reviewedBy=\"resync\" occurredAt=\"2026-03-04T05:06:07.000001Z\"\n"},
		{"o resync of one driver", func(f *fixture) { f.approved("driver-1"); f.approved("driver-2") }, func(f *fixture) {
			later(f, time.Hour)
			f.resync(map[string]string{"driverId": "driver-2"})
		}, "", "driver.application.reviewed key=driver-2 fields=driverId,eventId,occurredAt,reason,reviewedBy,status,vehicleType driverId=\"driver-2\" status=\"APPROVED\" vehicleType=\"CAR\" reason=\"resync\" reviewedBy=\"resync\" occurredAt=\"2026-03-04T06:06:07Z\"\n"},
		{"o resync queues nothing for a PENDING driver", gPending, func(f *fixture) {
			f.resync(map[string]string{"driverId": "driver-1"})
		}, "", ""},
		{"o a refused review queues nothing", gApproved, func(f *fixture) { f.decide("approve", "driver-1", "") }, "", ""},
		{"o apply queues nothing", gSaved, func(f *fixture) { f.apply("driver-1") }, "", ""},
		{"o rider update queues nothing", nil, func(f *fixture) {
			f.call("PUT", "/riders/rider-1", auth.Rider, "rider-1", map[string]string{"name": "Bảo"})
		}, "", ""},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			gCapture(t)
			f := gFix(t)
			if row.setup != nil {
				row.setup(f)
			}
			from := len(gQueued(t, f, 0))
			row.do(f)
			evs := gQueued(t, f, from)
			gEq(t, row.name, gSummary(t, evs), row.want)
			if row.rowTime != "" {
				var at time.Time
				if err := f.db.QueryRow(context.Background(), `SELECT reviewed_at FROM driver_profiles WHERE driver_id = $1`, row.rowTime).Scan(&at); err != nil {
					t.Fatal(err)
				}
				if len(evs) != 1 || evs[0].payload["occurredAt"] != at.UTC().Format(time.RFC3339Nano) {
					t.Errorf("occurredAt %v, want the row's reviewed_at %s", evs, at.UTC().Format(time.RFC3339Nano))
				}
			}
		})
	}
}
