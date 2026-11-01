import { getLatestOrder, getOrder, getTripPayments, waitForTripByOrder, type Order, type Trip } from "@grab-mini/client";
import type { ActiveRide, Outcome } from "./useRide";

const inFlight = (trip: Trip) => trip.status === "ASSIGNED" || trip.status === "IN_PROGRESS";

// order.matched can be pushed the instant before Trip Service has the trip, and
// the trip is looked up only for MATCHED orders, so a missing one is a real "no".
async function tripFor(orderId: string): Promise<Trip | null> {
  return waitForTripByOrder(orderId).catch(() => null);
}

/** How long after it ends a finished, unrated ride is still brought back after a reload. */
export const COMPLETED_RIDE_WINDOW_MS = 24 * 60 * 60 * 1000;

const DISMISSED_KEY = (riderId: string) => `grab-mini.rider.${riderId}.dismissedTrips`;
const DISMISSED_KEEP = 20;

function dismissedTrips(riderId: string): string[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(DISMISSED_KEY(riderId)) ?? "[]");
    return Array.isArray(parsed) ? parsed.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

/**
 * Remember that the rider is done with a finished trip (rated it or closed the screen), so a
 * reload does not bring its receipt and rating prompt back. Best effort: storage may be unavailable.
 */
export function dismissCompletedRide(riderId: string, tripId: string): void {
  try {
    const ids = [...dismissedTrips(riderId).filter((id) => id !== tripId), tripId].slice(-DISMISSED_KEEP);
    localStorage.setItem(DISMISSED_KEY(riderId), JSON.stringify(ids));
  } catch {
    // Private mode or a full quota: the worst case is the prompt shows once more.
  }
}

/**
 * Whether a COMPLETED trip is recent enough to show again. Neither Trip nor Order Service says
 * when a trip ended, so use when Payment Service settled it (moments after completion). A trip
 * not settled yet has just ended. If payments can't be read the age is unknown: say no.
 */
async function endedRecently(tripId: string): Promise<boolean> {
  try {
    const { payments } = await getTripPayments(tripId);
    const times = (payments ?? []).map((p) => Date.parse(p.createdAt)).filter((t) => !Number.isNaN(t));
    if (times.length === 0) return true;
    return Date.now() - Math.min(...times) <= COMPLETED_RIDE_WINDOW_MS;
  } catch {
    return false;
  }
}

/** The ride the rider is still in the middle of, or just finished and has yet to rate, if any. */
export async function findActiveRide(riderId: string): Promise<ActiveRide | null> {
  const order = await getLatestOrder(riderId);
  if (!order) return null;

  const base = {
    orderId: order.id,
    vehicleType: order.vehicleType,
    dropoff: order.dropoff,
    price: order.price,
    surgeMultiplier: order.surgeMultiplier,
    promoCode: order.promoCode,
    discount: order.discount,
  };

  if (order.status === "PENDING") return { ...base, phase: "searching" };
  if (order.status !== "MATCHED") return null;

  const trip = await tripFor(order.id);
  if (!trip) return null;
  if (trip.status === "COMPLETED") {
    if (trip.rating != null || dismissedTrips(riderId).includes(trip.id)) return null;
    if (!(await endedRecently(trip.id))) return null;
    return { ...base, phase: "completed", driverId: trip.driverId, tripId: trip.id };
  }
  if (!inFlight(trip)) return null;
  return {
    ...base,
    phase: trip.status === "IN_PROGRESS" ? "in_progress" : "assigned",
    driverId: trip.driverId,
    tripId: trip.id,
  };
}

/**
 * Where a search that was still pending has got to, or null if it's still
 * pending. A push normally delivers this first; asking is the safety net for a
 * push missed while the page was loading or the socket was reconnecting.
 */
export async function checkOutcome(orderId: string): Promise<Outcome | null> {
  const order: Order = await getOrder(orderId);
  if (order.status === "NO_DRIVER") return { kind: "no_driver" };
  if (order.status === "CANCELLED") return { kind: "cancelled", cancelledBy: order.cancelledBy ?? "" };
  if (order.status !== "MATCHED") return null;

  const trip = await tripFor(orderId);
  if (!trip) return null;
  return { kind: "matched", driverId: trip.driverId, vehicleType: order.vehicleType, tripId: trip.id };
}
