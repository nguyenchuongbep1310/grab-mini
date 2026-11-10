package domain

import "fmt"

// Redemption statuses, over-limit reasons and the trip status that gives a
// redemption back.
const (
	StatusRedeemed      = "REDEEMED"
	StatusOverLimit     = "OVER_LIMIT"
	StatusReleased      = "RELEASED"
	TripStatusCancelled = "CANCELLED"
)

// ValidateRedemption refuses an order.created that can never be applied.
func ValidateRedemption(orderID, riderID string, discount int64) error {
	if orderID == "" || riderID == "" {
		return &InvalidEvent{"order.created without orderId or riderId"}
	}
	if discount < 0 || discount > MaxAmount {
		return &InvalidEvent{fmt.Sprintf("discount %d is not an amount of VND", discount)}
	}
	return nil
}

// Redemption is how an order's use of a promotion is recorded. Status is what
// promotion.redeemed announces, StoredStatus what the row holds; Announced says
// whether anything is announced at all.
type Redemption struct {
	Announced    bool
	Status       string
	StoredStatus string
	Reason       string
}

// Decide records an order that already exists: it cannot be refused, only
// recorded for what it is. An order cancelled before we saw it placed is stored
// as RELEASED, not announced, and the usage is not consulted. Otherwise the
// limits decide between REDEEMED and OVER_LIMIT.
func Decide(p Promotion, u Usage, alreadyCancelled bool) Redemption {
	status, reason := StatusRedeemed, ""
	if !alreadyCancelled {
		if reason = p.OverLimitReason(u); reason != "" {
			status = StatusOverLimit
		}
	}
	stored := status
	if alreadyCancelled {
		stored = StatusReleased
	}
	return Redemption{Announced: !alreadyCancelled, Status: status, StoredStatus: stored, Reason: reason}
}
