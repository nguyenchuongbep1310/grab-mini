package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// What clients receive was pinned byte for byte before the Clean Architecture
// migration. The error responses are asserted in internal/adapters/http; the
// receipt is asserted here as well, because only a real booking shows that the
// response stored with an Idempotency-Key replays byte for byte.

// The response saved with an idempotent booking is replayed byte for byte,
// including rows written by an older binary, so its encoding is pinned: keys
// sorted, no promoCode unless there is one, and a trailing newline.
func TestTheReceiptEncodingIsStable(t *testing.T) {
	a := newAPI(t) // skips without TEST_DATABASE_URL

	got := a.book("rider-1", "k1", bookingBody)
	if got.Code != http.StatusCreated {
		t.Fatalf("booking = %d %s", got.Code, got.Raw)
	}
	price := bookingPrice()
	want := fmt.Sprintf(`{"currency":"VND","discount":0,"finalPrice":%d,"orderId":"%s","price":%d,"status":"PENDING","surgeMultiplier":1}`+"\n",
		price, got.OrderID, price)
	if got.Raw != want {
		t.Errorf("receipt = %q\nwant      %q", got.Raw, want)
	}

	replay := a.book("rider-1", "k1", bookingBody)
	if replay.Code != http.StatusCreated || !replay.Replay || replay.Raw != got.Raw {
		t.Errorf("replay = %d replayed=%v %q, want the stored body %q", replay.Code, replay.Replay, replay.Raw, got.Raw)
	}

	discount := price / 10
	promoted := a.book("rider-2", "k2", withToken(a.token("rider-2", price, "CAR", discount, t0.Add(-time.Minute))))
	if promoted.Code != http.StatusCreated {
		t.Fatalf("promo booking = %d %s", promoted.Code, promoted.Raw)
	}
	want = fmt.Sprintf(`{"currency":"VND","discount":%d,"finalPrice":%d,"orderId":"%s","price":%d,"promoCode":"WELCOME10","status":"PENDING","surgeMultiplier":1}`+"\n",
		discount, price-discount, promoted.OrderID, price)
	if promoted.Raw != want {
		t.Errorf("promo receipt = %q\nwant           %q", promoted.Raw, want)
	}

	replay = a.book("rider-2", "k2", withToken(a.token("rider-2", price, "CAR", discount, t0.Add(-time.Minute))))
	if replay.Code != http.StatusCreated || !replay.Replay || replay.Raw != promoted.Raw {
		t.Errorf("promo replay = %d replayed=%v %q, want the stored body %q", replay.Code, replay.Replay, replay.Raw, promoted.Raw)
	}
}
