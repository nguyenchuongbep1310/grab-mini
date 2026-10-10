import type { TripStatus, VehicleType } from "@grab-mini/events";
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
  /** PENDING until matching reports back, then MATCHED or NO_DRIVER. */
  status: "PENDING" | "MATCHED" | "NO_DRIVER";
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
}

export interface DriverRating {
  driverId: string;
  /** Mean of all ratings, or 0 when there are none. */
  average: number;
  count: number;
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

export function tripAction(tripId: string, action: TripAction): Promise<{ tripId: string; status: TripStatus }> {
  return request(`${TRIP_API}/trips/${tripId}/${action}`, { method: "POST" });
}

export function rateTrip(tripId: string, riderId: string, stars: number, comment = ""): Promise<{ tripId: string; stars: number }> {
  return request(`${TRIP_API}/trips/${tripId}/rating`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ riderId, stars, comment }),
  });
}

export function getDriverRating(driverId: string): Promise<DriverRating> {
  return request(`${RATING_API}/drivers/${driverId}/rating`);
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
