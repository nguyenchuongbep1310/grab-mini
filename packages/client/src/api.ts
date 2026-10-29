import type { CancelledBy, OrderStatus, TripStatus, VehicleType } from "@grab-mini/events";
import { forgetToken, getToken, isSignedIn } from "./auth";

export type { VehicleType };

// The dev servers proxy these prefixes to the Go services (see each app's
// vite.config.ts), so the browser only ever talks to its own origin.
const ORDER_API = "/order-api";
const TRIP_API = "/trip-api";
const RATING_API = "/rating-api";
const PRICING_API = "/pricing-api";

export interface LatLng {
  lat: number;
  lng: number;
}

export interface Order {
  id: string;
  riderId: string;
  pickup: LatLng;
  dropoff: LatLng;
  vehicleType: VehicleType;
  /** PENDING until matching reports back, then MATCHED or NO_DRIVER; CANCELLED if the ride was cancelled. */
  status: OrderStatus;
  /** Set once CANCELLED. */
  cancelledAt?: string;
  cancelledBy?: CancelledBy;
  cancelReason?: string;
  /** Surge multiplier the price includes; 1 when there was no surge. */
  surgeMultiplier?: number;
  /** Price locked at booking (VND); null for orders from before surge pricing. */
  price?: number | null;
}

/** A price offer from Pricing Service, good for booking until it expires. */
export interface Quote {
  quoteId: string;
  baseFare: number;
  multiplier: number;
  price: number;
  currency: string;
  /** ISO time. */
  expiresAt: string;
}

export interface Trip {
  id: string;
  orderId: string;
  driverId: string;
  /** Empty for trips from before the rider was recorded on them. */
  riderId?: string;
  status: TripStatus;
  /** Stars the rider gave, once they have. */
  rating?: number;
  /** Set once CANCELLED. */
  cancelledBy?: CancelledBy;
  cancelReason?: string;
}

export interface DriverRating {
  driverId: string;
  /** Mean of all ratings, or 0 when there are none. */
  average: number;
  count: number;
  /** Sum of recorded tips in VND (no money moves; it is a record). */
  totalTips: number;
  /** How many ratings carried a tip. */
  tipCount: number;
}

export interface DriverReview {
  tripId: string;
  stars: number;
  comment: string;
  /** Whole VND. */
  tipAmount: number;
  /** RFC 3339; pass the last one as `before` to get the next page. */
  createdAt: string;
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    /** The parsed response body, e.g. `{ newQuote }` on a 409 from POST /orders. */
    readonly body: Record<string, unknown> = {},
  ) {
    super(message);
  }
}

async function send(url: string, init: RequestInit | undefined, token: string | null): Promise<Response> {
  const headers = new Headers(init?.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(url, { ...init, headers });
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  let res = await send(url, init, isSignedIn() ? await getToken() : null);
  if (res.status === 401 && isSignedIn()) {
    // The token may have expired (or the server's clock disagrees): get a new one and try once more.
    forgetToken();
    res = await send(url, init, await getToken());
  }
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new ApiError(body.error ?? `Request failed (${res.status})`, res.status, body);
  return body as T;
}

/**
 * Books a ride. With a quoteId the price shown in the quote is honoured; if the
 * quote has expired and the price moved, this rejects with an ApiError of
 * status 409 whose `body.newQuote` is the price to confirm (see `newQuoteOf`).
 */
export function createOrder(
  riderId: string,
  pickup: LatLng,
  dropoff: LatLng,
  vehicleType: VehicleType,
  quoteId?: string,
): Promise<{ orderId: string; price?: number; surgeMultiplier?: number }> {
  return request(`${ORDER_API}/orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ riderId, pickup, dropoff, vehicleType, ...(quoteId ? { quoteId } : {}) }),
  });
}

/** The fresh quote carried by a 409 from createOrder, or null if err isn't one. */
export function newQuoteOf(err: unknown): Quote | null {
  if (err instanceof ApiError && err.status === 409 && err.body.newQuote) return err.body.newQuote as Quote;
  return null;
}

/** Price for a trip, with the current surge multiplier at the pickup. */
export function getQuote(pickup: LatLng, dropoff: LatLng, vehicleType: VehicleType): Promise<Quote> {
  const q = new URLSearchParams({
    pickupLat: String(pickup.lat),
    pickupLng: String(pickup.lng),
    dropoffLat: String(dropoff.lat),
    dropoffLng: String(dropoff.lng),
    vehicleType,
  });
  return request(`${PRICING_API}/quotes?${q}`);
}

export function getOrder(orderId: string): Promise<Order> {
  return request(`${ORDER_API}/orders/${orderId}`);
}

export function getTripByOrder(orderId: string): Promise<Trip> {
  return request(`${TRIP_API}/trips/by-order/${orderId}`);
}

/**
 * order.matched reaches the client at almost the same moment Trip Service
 * creates the trip, so the trip can briefly not exist yet. Retry the 404.
 */
export async function waitForTripByOrder(orderId: string, attempts = 8, delayMs = 400): Promise<Trip> {
  for (let i = 0; ; i++) {
    try {
      return await getTripByOrder(orderId);
    } catch (err) {
      const notYet = err instanceof ApiError && err.status === 404;
      if (!notYet || i >= attempts - 1) throw err;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

export type TripAction = "pickup" | "complete" | "cancel";

/** `reason` is only used by "cancel": an optional note on why. */
export function tripAction(
  tripId: string,
  action: TripAction,
  reason?: string,
): Promise<{ tripId: string; status: TripStatus }> {
  const note = action === "cancel" && reason ? reason : undefined;
  return request(`${TRIP_API}/trips/${tripId}/${action}`, {
    method: "POST",
    ...(note && { headers: { "Content-Type": "application/json" }, body: JSON.stringify({ reason: note }) }),
  });
}

/**
 * Cancels an order that has no driver yet. If a driver was matched in the
 * meantime the server answers 409 (cancel the trip instead); repeating the
 * call for an order already cancelled succeeds.
 */
export function cancelOrder(orderId: string, reason?: string): Promise<Order> {
  return request(`${ORDER_API}/orders/${orderId}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(reason ? { reason } : {}),
  });
}

export function rateTrip(
  tripId: string,
  riderId: string,
  stars: number,
  comment = "",
  tipAmount = 0,
): Promise<{ tripId: string; stars: number; tipAmount: number }> {
  return request(`${TRIP_API}/trips/${tripId}/rating`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ riderId, stars, comment, tipAmount }),
  });
}

export function getDriverRating(driverId: string): Promise<DriverRating> {
  return request(`${RATING_API}/drivers/${driverId}/rating`);
}

/** A driver's own reviews, newest first. Only that driver may read them. */
export async function getDriverReviews(driverId: string, opts: { limit?: number; before?: string } = {}): Promise<DriverReview[]> {
  const q = new URLSearchParams();
  if (opts.limit) q.set("limit", String(opts.limit));
  if (opts.before) q.set("before", opts.before);
  const qs = q.toString();
  const res = await request<{ reviews: DriverReview[] }>(`${RATING_API}/drivers/${driverId}/reviews${qs ? `?${qs}` : ""}`);
  return res.reviews;
}

/** The driver's trip that is still in flight, or null if they have none. */
export async function getActiveTrip(driverId: string): Promise<Trip | null> {
  try {
    return await request<Trip>(`${TRIP_API}/drivers/${driverId}/active-trip`);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

/** The rider's most recent order, or null if they have never booked. */
export async function getLatestOrder(riderId: string): Promise<Order | null> {
  try {
    return await request<Order>(`${ORDER_API}/riders/${riderId}/latest-order`);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

// ---- Payment Service ------------------------------------------------------
// Amounts are whole VND (integer minor units; VND has none).
const PAYMENT_API = "/payment-api";

export type PaymentKind = "FARE" | "TIP" | "CANCELLATION_FEE";

/** What a rider was charged for one trip. */
export interface RiderPayment {
  id: string;
  kind: PaymentKind;
  /** DECLINED when the payment provider refused the rider's payment method. */
  status: "CAPTURED" | "DECLINED";
  amount: number;
  currency: string;
  createdAt: string;
}

/** What a driver earns from one charge: their share, the gross and the platform's commission. */
export interface DriverPayout {
  id: string;
  kind: PaymentKind;
  status: "CAPTURED";
  amount: number;
  gross: number;
  commission: number;
  currency: string;
  createdAt: string;
}

export interface DriverEarnings {
  driverId: string;
  currency: string;
  /** The driver's share of every captured payment (fares + tips + cancellation fees). */
  total: number;
  fares: number;
  tips: number;
  cancellationFees: number;
  commission: number;
  /** Trips with a captured fare. */
  trips: number;
  /** What the platform currently owes the driver, from the ledger (all time). */
  payableBalance: number;
}

/**
 * The charges of one trip as the caller sees them: a rider gets `payments`, a
 * driver `payouts`. Both are empty until Payment Service has settled the trip,
 * so callers poll briefly after a trip completes.
 */
export function getTripPayments(
  tripId: string,
): Promise<{ tripId: string; currency: string; total: number; payments?: RiderPayment[]; payouts?: DriverPayout[] }> {
  return request(`${PAYMENT_API}/payments?tripId=${encodeURIComponent(tripId)}`);
}

/** A driver's own earnings summary; optional RFC 3339 `from` (inclusive) and `to` (exclusive). */
export function getDriverEarnings(driverId: string, range: { from?: string; to?: string } = {}): Promise<DriverEarnings> {
  const q = new URLSearchParams();
  if (range.from) q.set("from", range.from);
  if (range.to) q.set("to", range.to);
  const qs = q.toString();
  return request(`${PAYMENT_API}/drivers/${driverId}/earnings${qs ? `?${qs}` : ""}`);
}
