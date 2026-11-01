import { closeReceipt, getLatestOrder, getOrder, waitForTripByOrder, type Order, type Trip } from "@grab-mini/client";
import type { ActiveRide, Outcome } from "./useRide";

const inFlight = (trip: Trip) => trip.status === "ASSIGNED" || trip.status === "IN_PROGRESS";

// order.matched can be pushed the instant before Trip Service has the trip, and
// the trip is looked up only for MATCHED orders, so a missing one is a real "no".
async function tripFor(orderId: string): Promise<Trip | null> {
  return waitForTripByOrder(orderId).catch(() => null);
}

/** How long after it ends a finished, unrated ride is still brought back after a reload. */
export const COMPLETED_RIDE_WINDOW_MS = 24 * 60 * 60 * 1000;

const CLOSE_RETRIES = 2;
const CLOSE_RETRY_DELAY_MS = 1500;

/**
 * Tell Trip Service the rider is done with a finished trip's receipt, so no device brings it back.
 * Fire and forget: the caller has already moved on. A failure is retried a couple of times after a
 * short delay and then dropped; the worst case is that the receipt shows once more after a reload.
 */
export async function closeCompletedRide(tripId: string): Promise<void> {
  for (let attempt = 0; attempt <= CLOSE_RETRIES; attempt++) {
    try {
      await closeReceipt(tripId);
      return;
    } catch {
      if (attempt < CLOSE_RETRIES) await new Promise((r) => setTimeout(r, CLOSE_RETRY_DELAY_MS));
    }
  }
}

/**
 * Whether a COMPLETED trip ended recently enough to show again, by the completedAt Trip Service
 * returns. Missing or unparseable means the age is unknown: say no, never guess. A time slightly
 * in the future (clock skew between server and browser) counts as recent.
 */
function endedRecently(trip: Trip): boolean {
  const endedAt = trip.completedAt ? Date.parse(trip.completedAt) : NaN;
  return !Number.isNaN(endedAt) && Date.now() - endedAt <= COMPLETED_RIDE_WINDOW_MS;
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
    if (trip.rating != null || trip.receiptClosedAt) return null;
    if (!endedRecently(trip)) return null;
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
