import { getLatestOrder, getOrder, waitForTripByOrder, type Order, type Trip } from "@grab-mini/client";
import type { ActiveRide, Outcome } from "./useRide";

const inFlight = (trip: Trip) => trip.status === "ASSIGNED" || trip.status === "IN_PROGRESS";

// order.matched can be pushed the instant before Trip Service has the trip, and
// the trip is looked up only for MATCHED orders, so a missing one is a real "no".
async function tripFor(orderId: string): Promise<Trip | null> {
  return waitForTripByOrder(orderId).catch(() => null);
}

/** The ride the rider is still in the middle of, if any. */
export async function findActiveRide(riderId: string): Promise<ActiveRide | null> {
  const order = await getLatestOrder(riderId);
  if (!order) return null;

  const base = { orderId: order.id, vehicleType: order.vehicleType, dropoff: order.dropoff };

  if (order.status === "PENDING") return { ...base, phase: "searching" };
  if (order.status !== "MATCHED") return null;

  const trip = await tripFor(order.id);
  if (!trip || !inFlight(trip)) return null;
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
