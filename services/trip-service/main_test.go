package main

import (
	"grab-mini/pkg/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
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
		"negative tip":     `{"stars":5,"tipAmount":-1}`,
		"tip over the cap": `{"stars":5,"tipAmount":1000001}`,
		"fractional tip":   `{"stars":5,"tipAmount":1.5}`,
		"string tip":       `{"stars":5,"tipAmount":"10000"}`,
		"control chars":    `{"stars":5,"comment":"a\u0000b"}`,
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

func TestRateTripTipBoundsAndCommentAreAccepted(t *testing.T) {
	for _, tip := range []string{"0", "10000", "1000000"} {
		if rec := rate("not-a-uuid", `{"stars":5,"tipAmount":`+tip+`}`); rec.Code != http.StatusNotFound {
			t.Errorf("tip=%s: status = %d, want 404 (accepted, then trip not found)", tip, rec.Code)
		}
	}
	// Multi-line comments are fine.
	if rec := rate("not-a-uuid", `{"stars":5,"comment":"a\nb"}`); rec.Code != http.StatusNotFound {
		t.Errorf("multi-line comment: status = %d, want 404", rec.Code)
	}
}

func TestNormalizeRatingTrimsAndCountsRunesAfterTrimming(t *testing.T) {
	req := rateRequest{Stars: 5, Comment: "  " + strings.Repeat("é", 500) + "  "}
	if msg := normalizeRating(&req); msg != "" {
		t.Fatalf("500 runes plus padding rejected: %s", msg)
	}
	if utf8.RuneCountInString(req.Comment) != 500 {
		t.Errorf("comment not trimmed: %d runes", utf8.RuneCountInString(req.Comment))
	}
	req = rateRequest{Stars: 5, Comment: strings.Repeat("é", 501)}
	if normalizeRating(&req) == "" {
		t.Error("501 runes accepted")
	}
}
