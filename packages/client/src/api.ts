import type { CancelledBy, ChatMessageSent, ChatRole, OrderStatus, TripStatus, VehicleType } from "@grab-mini/events";
import { forgetToken, getToken, isSignedIn } from "./auth";

export type { VehicleType };

// The dev servers proxy these prefixes to the Go services (see each app's
// vite.config.ts), so the browser only ever talks to its own origin.
const ORDER_API = "/order-api";
const TRIP_API = "/trip-api";
const RATING_API = "/rating-api";
const PRICING_API = "/pricing-api";
const ROUTING_API = "/routing-api";
const PROMOTION_API = "/promotion-api";
const CHAT_API = "/chat-api";
const PROFILE_API = "/profile-api";

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
  /** The promo code the order was booked with; absent for none. */
  promoCode?: string;
  /** VND the promo code takes off `price` (0 for none), locked at booking. */
  discount?: number;
  /** What the rider pays: `price - discount`; null with `price`. */
  finalPrice?: number | null;
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

/** A route from Routing Service: a model (great-circle distance x 1.3, speed by hour), not maps data. */
export interface Route {
  /** Road distance estimate, km. */
  distanceKm: number;
  /** Travel time estimate, seconds. */
  durationSec: number;
  /** A rough straight-line path from the origin to the destination. */
  polyline: LatLng[];
  vehicle: VehicleType;
  /** Which version of the model produced this, e.g. "detour-1.3". */
  model: string;
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
 * With a promoToken (from `validatePromotion`) its discount is locked on the
 * order; a token that is expired, for another price or already used rejects
 * with an ApiError whose `body.code` says why (see `promoBookingErrorOf`).
 */
export async function createOrder(
  riderId: string,
  pickup: LatLng,
  dropoff: LatLng,
  vehicleType: VehicleType,
  quoteId?: string,
  retryDelayMs = 300,
  promoToken?: string,
): Promise<{ orderId: string; price?: number; surgeMultiplier?: number; discount?: number; finalPrice?: number; promoCode?: string }> {
  // One key per booking attempt, reused by every retry of it, so a request that
  // reached the server but whose answer was lost books one order, not two.
  const init: RequestInit = {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": newIdempotencyKey() },
    body: JSON.stringify({
      riderId,
      pickup,
      dropoff,
      vehicleType,
      ...(quoteId ? { quoteId } : {}),
      ...(promoToken ? { promoToken } : {}),
    }),
  };
  const maxAttempts = 3;
  for (let attempt = 1; ; attempt++) {
    try {
      return await request(`${ORDER_API}/orders`, init);
    } catch (err) {
      // Retry only what may not have reached the server or may succeed later:
      // a network failure (fetch rejects) or a 5xx. A 4xx is an answer.
      const retryable = err instanceof ApiError ? err.status >= 500 : err instanceof TypeError;
      if (!retryable || attempt >= maxAttempts) throw err;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs * attempt));
    }
  }
}

function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}-${Math.random().toString(36).slice(2)}`;
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

/**
 * Distance and travel time between two points. A 422 (ApiError) means the trip
 * is beyond what the model covers (200 km).
 */
export function getRoute(from: LatLng, to: LatLng, vehicle: VehicleType): Promise<Route> {
  const q = new URLSearchParams({
    fromLat: String(from.lat),
    fromLng: String(from.lng),
    toLat: String(to.lat),
    toLng: String(to.lng),
    vehicle,
  });
  return request(`${ROUTING_API}/route?${q}`);
}

export function getOrder(orderId: string): Promise<Order> {
  return request(`${ORDER_API}/orders/${orderId}`);
}

/**
 * A driver may read an order only once Order Service has recorded the match,
 * which it learns from the same order.matched event the driver was just told
 * about, so the order can briefly be a 404. Retry that.
 */
export async function waitForOrder(orderId: string, attempts = 8, delayMs = 400): Promise<Order> {
  for (let i = 0; ; i++) {
    try {
      return await getOrder(orderId);
    } catch (err) {
      const notYet = err instanceof ApiError && err.status === 404;
      if (!notYet || i >= attempts - 1) throw err;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
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

/**
 * What cancelling costs a rider once a driver was assigned: `fee` (VND) applies only when the
 * cancel comes more than `graceSeconds` after the assignment. A `fee` of 0 means it is free.
 */
export interface CancellationPolicy {
  currency: string;
  fee: number;
  graceSeconds: number;
}

export function getCancellationPolicy(): Promise<CancellationPolicy> {
  return request(`${PAYMENT_API}/cancellation-policy`);
}

/** A driver's own earnings summary; optional RFC 3339 `from` (inclusive) and `to` (exclusive). */
export function getDriverEarnings(driverId: string, range: { from?: string; to?: string } = {}): Promise<DriverEarnings> {
  const q = new URLSearchParams();
  if (range.from) q.set("from", range.from);
  if (range.to) q.set("to", range.to);
  const qs = q.toString();
  return request(`${PAYMENT_API}/drivers/${driverId}/earnings${qs ? `?${qs}` : ""}`);
}

// ---- Promotion Service ----------------------------------------------------
// Amounts are whole VND. Riders check a code before booking; admins manage promotions.

export type PromotionKind = "PERCENT" | "FIXED";

/** Why a code cannot be used for this trip: the `code` of a 404/422 from `validatePromotion`. */
export type PromoRejectionCode =
  | "not_found"
  | "not_started"
  | "expired"
  | "min_fare"
  | "wrong_vehicle"
  | "limit_reached"
  | "already_used";

/** A code that applies to a trip: the discount, and a signed token to book with (valid ~10 minutes). */
export interface PromoValidation {
  code: string;
  /** VND taken off `price`. */
  discount: number;
  price: number;
  /** `price - discount`, never below 0. */
  finalPrice: number;
  currency: string;
  /** Pass to `createOrder`; bound to this rider, price and vehicle type. */
  promoToken: string;
  /** ISO time the token stops working. */
  expiresAt: string;
}

const PROMO_REJECTIONS: readonly string[] = [
  "not_found",
  "not_started",
  "expired",
  "min_fare",
  "wrong_vehicle",
  "limit_reached",
  "already_used",
];

/**
 * Checks a promo code for a trip priced at `price` (VND). Rejects with an ApiError
 * (404 or 422) when the code cannot be used; `promoRejectionOf` gives the reason.
 */
export function validatePromotion(code: string, price: number, vehicleType: VehicleType): Promise<PromoValidation> {
  const q = new URLSearchParams({ code: code.trim(), price: String(Math.round(price)), vehicleType });
  return request(`${PROMOTION_API}/promotions/validate?${q}`);
}

/** The machine-readable reason a code was refused by `validatePromotion`, or null if err isn't one. */
export function promoRejectionOf(err: unknown): PromoRejectionCode | null {
  if (!(err instanceof ApiError) || (err.status !== 404 && err.status !== 422)) return null;
  const code = err.body.code;
  return typeof code === "string" && PROMO_REJECTIONS.includes(code) ? (code as PromoRejectionCode) : null;
}

/** Why `createOrder` refused a promo token (a 400, or a 409 for promo_used). */
export type PromoBookingErrorCode =
  | "promo_invalid"
  | "promo_expired"
  | "promo_wrong_rider"
  | "promo_wrong_price"
  | "promo_wrong_vehicle"
  | "promo_used";

/** The reason `createOrder` refused the promo token, or null if err isn't about one. Apply the code again. */
export function promoBookingErrorOf(err: unknown): PromoBookingErrorCode | null {
  if (!(err instanceof ApiError) || (err.status !== 400 && err.status !== 409)) return null;
  const code = err.body.code;
  return typeof code === "string" && code.startsWith("promo_") ? (code as PromoBookingErrorCode) : null;
}

/** A promotion as an admin sees it. */
export interface Promotion {
  code: string;
  kind: PromotionKind;
  /** A percentage (1 to 100) for PERCENT, VND for FIXED. */
  value: number;
  /** Cap on a PERCENT discount, VND; null for none. */
  maxDiscount: number | null;
  /** The trip price must be at least this (VND). */
  minFare: number;
  /** Null: any vehicle. */
  vehicleType: VehicleType | null;
  validFrom: string;
  validTo: string;
  /** Null: unlimited. A soft limit: orders in flight can pass it (they show as `overLimit`). */
  usageLimit: number | null;
  perRiderLimit: number;
  disabledAt: string | null;
  status: "scheduled" | "active" | "expired" | "disabled";
  /** Redemptions that count against the limits. */
  redeemed: number;
  /** Orders recorded past a limit. */
  overLimit: number;
  createdBy: string;
  createdAt: string;
}

/** What an admin supplies to create a promotion. */
export interface NewPromotion {
  /** 3 to 32 characters: letters, digits, "-" or "_" (stored upper case). */
  code: string;
  kind: PromotionKind;
  value: number;
  /** PERCENT only. */
  maxDiscount?: number;
  minFare?: number;
  vehicleType?: VehicleType;
  /** ISO time; defaults to now. */
  validFrom?: string;
  /** ISO time, required. */
  validTo: string;
  usageLimit?: number;
  /** Defaults to 1. */
  perRiderLimit?: number;
}

/** Admin only. Every promotion, newest first. */
export async function listPromotions(): Promise<Promotion[]> {
  const body = await request<{ promotions: Promotion[] }>(`${PROMOTION_API}/promotions`);
  return body.promotions;
}

/** Admin only. A 400 (`body.code` "invalid_promotion") says what is wrong; a 409 means the code exists. */
export function createPromotion(promotion: NewPromotion): Promise<Promotion> {
  return request(`${PROMOTION_API}/promotions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(promotion),
  });
}

/** Admin only. Stops new uses of a code; orders that already carry its discount keep it. Repeating it is harmless. */
export function disablePromotion(code: string): Promise<Promotion> {
  return request(`${PROMOTION_API}/promotions/${encodeURIComponent(code)}/disable`, { method: "POST" });
}

// ---- Chat Service ---------------------------------------------------------
// The rider and the driver of a trip exchange short messages. Only the two of them may read or send;
// the text is personal data (do not log it). Pushes arrive on the notification socket as
// `chat.message.sent` (see `chatMessageFromPush`); the history below is how to recover what a push missed.

/** A chat message as the API returns it. */
export interface ChatMessage {
  id: string;
  tripId: string;
  senderId: string;
  senderRole: ChatRole;
  text: string;
  /** ISO time, to the microsecond. */
  createdAt: string;
}

/** One page of a trip's history, newest first. */
export interface ChatHistory {
  messages: ChatMessage[];
  /** Pass as `before` to get the next older page; null when this was the last page. */
  nextBefore: string | null;
  tripStatus: TripStatus;
  /** False once the trip has ended and the grace period (10 minutes by default) has passed: read-only. */
  open: boolean;
  /** ISO time the chat becomes read-only; null while the trip is still on. */
  closesAt: string | null;
}

/** Longest message, in characters (not bytes): longer text is refused with `invalid_text`. */
export const CHAT_MAX_LENGTH = 500;

/** The `code` of a refusal from Chat Service. */
export type ChatErrorCode =
  | "invalid_text" // empty, over 500 characters, or containing a control character (newlines included)
  | "invalid_request"
  | "not_participant" // 403: the caller is not the rider or driver of this trip
  | "unknown_trip" // 404: the trip is not known to chat yet (just created) or no longer
  | "chat_closed" // 409: the trip ended more than the grace period ago
  | "no_recipient" // 409: the other person on the trip is not known
  | "key_mismatch" // 422: the Idempotency-Key was used with other text
  | "rate_limited"; // 429: see `chatRetryAfterOf`

const CHAT_ERRORS: readonly string[] = [
  "invalid_text",
  "invalid_request",
  "not_participant",
  "unknown_trip",
  "chat_closed",
  "no_recipient",
  "key_mismatch",
  "rate_limited",
];

/** Why Chat Service refused a request, or null if err isn't one of its refusals. */
export function chatErrorOf(err: unknown): ChatErrorCode | null {
  if (!(err instanceof ApiError)) return null;
  const code = err.body.code;
  return typeof code === "string" && CHAT_ERRORS.includes(code) ? (code as ChatErrorCode) : null;
}

/** Seconds to wait after a `rate_limited` refusal, or null if err isn't one. */
export function chatRetryAfterOf(err: unknown): number | null {
  if (chatErrorOf(err) !== "rate_limited" || !(err instanceof ApiError)) return null;
  const n = err.body.retryAfter;
  return typeof n === "number" && n > 0 ? n : 1;
}

/**
 * Sends a message on a trip. The text is trimmed by the server; it must be 1 to 500
 * characters with no control characters. Rejects with an ApiError whose `body.code` says
 * why (see `chatErrorOf`).
 *
 * One Idempotency-Key per call, reused by its retries (a network failure or a 5xx), so a
 * request that reached the server but whose answer was lost sends one message, not two.
 * To retry a failed send yourself later (after the user taps "retry"), pass the same `idempotencyKey`.
 */
export async function sendChatMessage(
  tripId: string,
  text: string,
  opts: { idempotencyKey?: string; retryDelayMs?: number } = {},
): Promise<ChatMessage> {
  const init: RequestInit = {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": opts.idempotencyKey ?? newIdempotencyKey() },
    body: JSON.stringify({ text }),
  };
  const retryDelayMs = opts.retryDelayMs ?? 300;
  const maxAttempts = 3;
  for (let attempt = 1; ; attempt++) {
    try {
      return await request(`${CHAT_API}/trips/${encodeURIComponent(tripId)}/messages`, init);
    } catch (err) {
      const retryable = err instanceof ApiError ? err.status >= 500 : err instanceof TypeError;
      if (!retryable || attempt >= maxAttempts) throw err;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs * attempt));
    }
  }
}

/**
 * A page of a trip's messages, newest first. Start without `before`; for older ones pass the
 * previous page's `nextBefore`. `limit` is 1 to 100 (default 50). Only the trip's rider and driver may read.
 */
export function listChatMessages(tripId: string, opts: { before?: string; limit?: number } = {}): Promise<ChatHistory> {
  const q = new URLSearchParams();
  if (opts.limit) q.set("limit", String(opts.limit));
  if (opts.before) q.set("before", opts.before);
  const qs = q.toString();
  return request(`${CHAT_API}/trips/${encodeURIComponent(tripId)}/messages${qs ? `?${qs}` : ""}`);
}

/** A pushed `chat.message.sent` as a message, to append to the history (dedupe by `id`: a push and a reload can overlap). */
export function chatMessageFromPush(push: ChatMessageSent): ChatMessage {
  return {
    id: push.messageId,
    tripId: push.tripId,
    senderId: push.senderId,
    senderRole: push.senderRole,
    text: push.text,
    createdAt: push.occurredAt,
  };
}

// ---- Profile Service ------------------------------------------------------
// Who people are, and who may drive. Names, phones and plates are personal data (do not log them).
// A rider or driver reads and writes only their own profile (the id is the token's subject); a driver
// applies, an admin reviews. A driver who is not APPROVED is offered no trips.

/** NONE: saved a profile, never applied. PENDING: waiting for an admin. */
export type DriverStatus = "NONE" | "PENDING" | "APPROVED" | "REJECTED" | "SUSPENDED";

/** A rider's profile; empty strings and a null `updatedAt` until they save one. */
export interface RiderProfile {
  riderId: string;
  name: string;
  phone: string;
  updatedAt: string | null;
}

/** `name` is required (1 to 80 characters, with a letter); `phone` is optional (7 to 15 digits, loosely checked). */
export interface RiderProfileInput {
  name: string;
  phone?: string;
}

/**
 * A driver's own profile and onboarding status. `vehicleType` is "" until they pick one. A driver who
 * never saved anything has status NONE and empty fields. `reason` is why they were rejected or suspended
 * (or why a vehicle change put them back under review).
 */
export interface DriverProfile {
  driverId: string;
  name: string;
  phone: string;
  vehicleType: VehicleType | "";
  plate: string;
  model: string;
  color: string;
  status: DriverStatus;
  reason: string;
  /** ISO time of the last decision, or null. */
  reviewedAt: string | null;
  /** ISO time the status last changed, or null for a driver with no profile. */
  statusAt: string | null;
  updatedAt: string | null;
}

/**
 * What PUT /drivers/{id} replaces (the status is never taken from it). Only `name` is required to save a
 * draft; `vehicleType` and `plate` are required to apply, and cannot be blanked once the driver has applied.
 * `plate` is stored in capitals. Changing the plate or vehicle type of an APPROVED driver puts them back to
 * PENDING (out of service until an admin approves the new vehicle): check the returned `status`.
 */
export interface DriverProfileInput {
  name: string;
  phone?: string;
  vehicleType?: VehicleType;
  plate?: string;
  model?: string;
  color?: string;
}

/** What anyone signed in may see of an APPROVED driver (never the phone), e.g. on the rider's matched card. */
export interface DriverPublic {
  driverId: string;
  name: string;
  vehicleType: VehicleType;
  plate: string;
  model: string;
  color: string;
}

/** An application as an admin sees it: the whole profile (phone included) and who decided. */
export interface DriverApplication extends DriverProfile {
  reviewedBy: string;
}

export interface DriverApplicationPage {
  applications: DriverApplication[];
  /** Pass as `cursor` for the next page; null when this was the last. */
  nextCursor: string | null;
}

export type ReviewAction = "approve" | "reject" | "suspend";

/** The `code` of a refusal from Profile Service. */
export type ProfileErrorCode =
  | "invalid_profile" // 400: `body.field` names the field ("name", "phone", "plate", "vehicleType", "model", "color", "reason")
  | "invalid_request" // 400: not JSON, or a bad query
  | "forbidden" // 403: someone else's profile
  | "not_found" // 404
  | "incomplete_profile" // 422 applying without a name, vehicle type or plate (`body.field` is the first missing one)
  | "suspended" // 409 applying while suspended
  | "invalid_transition" // 409 review not allowed from the driver's status (`body.status` is the status they have)
  | "rate_limited"; // 429

const PROFILE_ERRORS: readonly string[] = [
  "invalid_profile",
  "invalid_request",
  "forbidden",
  "not_found",
  "incomplete_profile",
  "suspended",
  "invalid_transition",
  "rate_limited",
];

/** Why Profile Service refused a request, or null if err isn't one of its refusals. */
export function profileErrorOf(err: unknown): ProfileErrorCode | null {
  if (!(err instanceof ApiError)) return null;
  const code = err.body.code;
  return typeof code === "string" && PROFILE_ERRORS.includes(code) ? (code as ProfileErrorCode) : null;
}

/** The field a refusal is about (`invalid_profile`, `incomplete_profile`), or null. */
export function profileFieldOf(err: unknown): string | null {
  if (profileErrorOf(err) === null || !(err instanceof ApiError)) return null;
  const field = err.body.field;
  return typeof field === "string" ? field : null;
}

function putJson<T>(url: string, body: unknown): Promise<T> {
  return request(url, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
}

/** The signed-in rider's profile (their own `riderId`); empty on the first visit, not a 404. */
export function getRiderProfile(riderId: string): Promise<RiderProfile> {
  return request(`${PROFILE_API}/riders/${encodeURIComponent(riderId)}`);
}

/** Replaces the rider's name and phone. A 400 (`invalid_profile`) names the field. */
export function updateRiderProfile(riderId: string, profile: RiderProfileInput): Promise<RiderProfile> {
  return putJson(`${PROFILE_API}/riders/${encodeURIComponent(riderId)}`, profile);
}

/** The signed-in driver's own profile and status; status NONE with empty fields on the first visit. */
export function getDriverProfile(driverId: string): Promise<DriverProfile> {
  return request(`${PROFILE_API}/drivers/${encodeURIComponent(driverId)}`);
}

/** Replaces the driver's profile fields; see `DriverProfileInput` for what is required and the effect of a vehicle change. */
export function updateDriverProfile(driverId: string, profile: DriverProfileInput): Promise<DriverProfile> {
  return putJson(`${PROFILE_API}/drivers/${encodeURIComponent(driverId)}`, profile);
}

/**
 * Submits the saved profile for review: status becomes PENDING. Repeating it is harmless (an APPROVED or
 * PENDING driver is returned unchanged). Rejects with `incomplete_profile` (422) if the name, vehicle type or
 * plate is missing, and `suspended` (409) if an admin suspended the driver.
 */
export function applyAsDriver(driverId: string): Promise<DriverProfile> {
  return request(`${PROFILE_API}/drivers/${encodeURIComponent(driverId)}/application`, { method: "POST" });
}

/**
 * The name, vehicle and plate of an APPROVED driver, for any signed-in user; null when there is no such
 * approved driver (404: unknown, not approved and suspended look the same) so a card can fall back to the id.
 * Other failures (429 rate limit, network) reject.
 */
export async function getDriverPublic(driverId: string): Promise<DriverPublic | null> {
  try {
    return await request<DriverPublic>(`${PROFILE_API}/drivers/${encodeURIComponent(driverId)}/public`);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

/**
 * Admin only. Drivers with one status (default PENDING), the ones waiting longest first. Pass the previous
 * page's `nextCursor` as `cursor`; `limit` is 1 to 100 (default 50).
 */
export function listDriverApplications(
  opts: { status?: Exclude<DriverStatus, "NONE">; limit?: number; cursor?: string } = {},
): Promise<DriverApplicationPage> {
  const q = new URLSearchParams();
  if (opts.status) q.set("status", opts.status);
  if (opts.limit) q.set("limit", String(opts.limit));
  if (opts.cursor) q.set("cursor", opts.cursor);
  const qs = q.toString();
  return request(`${PROFILE_API}/driver-applications${qs ? `?${qs}` : ""}`);
}

/**
 * Admin only. approve (PENDING or SUSPENDED to APPROVED), reject (PENDING to REJECTED) or suspend (APPROVED to
 * SUSPENDED). `reason` (1 to 500 characters, shown to the driver) is required to reject and suspend. A status that
 * does not allow the action rejects with `invalid_transition` (409), whose `body.status` is the status they have.
 */
export function reviewDriver(driverId: string, action: ReviewAction, reason?: string): Promise<DriverApplication> {
  return request(`${PROFILE_API}/drivers/${encodeURIComponent(driverId)}/${action}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(reason === undefined ? {} : { reason }),
  });
}
