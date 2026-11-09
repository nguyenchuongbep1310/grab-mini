package domain

// Discount is what the promotion takes off a trip priced at price, in whole VND.
//
//   - PERCENT: price * value / 100 rounded down (a rider is never given a
//     fraction of a dong more than the percentage), then capped at MaxDiscount.
//   - FIXED: Value.
//
// The result is never negative and never more than price, so the final price
// (price - discount) is never below 0. It does not look at eligibility; see Check.
func (p Promotion) Discount(price int64) int64 {
	if price <= 0 {
		return 0
	}
	var d int64
	switch p.Kind {
	case KindPercent:
		pct := min(max(p.Value, 0), 100)
		d = price * pct / 100
		if p.MaxDiscount != nil {
			d = min(d, *p.MaxDiscount)
		}
	case KindFixed:
		d = p.Value
	}
	return min(max(d, 0), price)
}
