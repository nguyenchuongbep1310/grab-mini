package main

import (
	"grab-mini/pkg/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Validation runs before the database or Kafka is touched, so a server with
// no dependencies can test what gets rejected.
func rate(id, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/trips/"+id+"/rating", strings.NewReader(body))
	req.SetPathValue("id", id)
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "r", Role: auth.Rider}))
	(&server{}).handleRateTrip(rec, req)
	return rec
}

const someTrip = "6f9619ff-8b86-d011-b42d-00c04fc964ff"

func TestRateTripRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"not JSON":         `nope`,
		"zero stars":       `{"riderId":"r","stars":0}`,
		"six stars":        `{"riderId":"r","stars":6}`,
		"negative stars":   `{"riderId":"r","stars":-1}`,
		"missing stars":    `{"riderId":"r"}`,
		"comment too long": `{"riderId":"r","stars":5,"comment":"` + strings.Repeat("x", 501) + `"}`,
	}
	for name, body := range cases {
		if rec := rate(someTrip, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestRateTripAcceptsTheBoundsOfTheScale(t *testing.T) {
	// Valid input gets past validation and then needs the database, which
	// this test doesn't have. Reaching a 404 for a malformed id proves the
	// body was accepted first (validation precedes the id check).
	for _, stars := range []string{"1", "5"} {
		rec := rate("not-a-uuid", `{"riderId":"r","stars":`+stars+`}`)
		if rec.Code != http.StatusNotFound {
			t.Errorf("stars=%s: status = %d, want 404 (accepted, then trip not found)", stars, rec.Code)
		}
	}
}

func TestRateTripCommentAtTheLimitIsAccepted(t *testing.T) {
	rec := rate("not-a-uuid", `{"riderId":"r","stars":4,"comment":"`+strings.Repeat("é", 500)+`"}`)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404: 500 characters (not bytes) should be allowed", rec.Code)
	}
}
