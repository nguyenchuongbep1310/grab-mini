# 0011. A signed promo token instead of a call to Promotion Service at booking

Status: accepted

## Context

A rider enters a promo code and the order must carry the discount. Order Service owns the booking path, which must stay available and fast. Promotion Service owns the rules (validity, limits, minimum fare).

## Decision

Validation and booking are separate steps. `GET /promotions/validate` (Promotion Service) checks the code and returns a **signed promo token** (`pkg/promo`): the body names the rider, code, price, vehicle type and discount, followed by an HMAC-SHA256, valid for 10 minutes. Order Service **verifies the token locally** and locks the discount onto the order; it never calls Promotion Service when booking.

- The key is `PROMO_SECRET`, separate from the quote key and with a domain-separated MAC, so whoever can sign price quotes cannot sign discounts.
- A token is bound to one rider, one price and one vehicle. Another rider, a changed price or the wrong vehicle is refused with a specific code.
- A token buys one booking: Order Service marks it used in Redis (SET NX) and gives it back if the order is not saved. If Redis is down the discount is still applied and counted (`orders_promo_total{result="unchecked"}`).
- `order.created` v4 carries `promoCode` and `discount` (defaulted, BACKWARD compatible). Promotion Service records the redemption from that event, and releases it on `order.cancelled`, on a cancelled trip and on `order.no_driver_found`.
- Payment charges `price - discount`, with commission on the net amount.

## Alternatives considered

- **Order Service calls Promotion Service when booking.** Simplest and exact, but a promotion outage would block bookings ([ADR 0001](0001-kafka-event-driven-architecture.md)).
- **Promotion Service reserves the code at validation.** Exact limits, but needs a release path for every abandoned booking.
- **Put the discount in the quote.** Couples promotions to pricing and its 60-second lifetime.

## Consequences

- Good: booking has no new runtime dependency; a forged or reused token is refused locally; the same pattern as price quotes, so there is one idea to learn.
- Bad: **the global usage limit is soft.** Two bookings in flight can both validate against the last remaining use, so a limit can be exceeded by the number of in-flight orders. The redemption is then recorded as `OVER_LIMIT`, never failed, because the rider already saw the price. The per-rider limit is exact because it is enforced inside the redemption transaction.
- Bad: a token cannot be revoked once issued, only expire. Disabling a code stops new validations, not tokens already issued (at most 10 minutes).
- Bad: another shared secret to distribute and rotate.

## Revisit when

A promotion has a hard budget (for example a fixed pool of credits). Then reserve at validation, with a release on timeout.
