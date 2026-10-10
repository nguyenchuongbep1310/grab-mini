import { useReducer } from "react";
import type { CancelledBy, NotificationMessage, VehicleType } from "@grab-mini/events";
import type { LatLng } from "@grab-mini/client";

export type Phase =
  | "idle"
  | "booking"
  | "searching"
  | "assigned"
  | "in_progress"
  | "completed"
  | "cancelled"
  | "no_driver";

/** Timeline step a ride has reached; used to show where a cancellation happened. */
export type Step = "searching" | "assigned" | "in_progress" | "completed";

export interface RideState {
  phase: Phase;
  reached: Step;
  dropoff: LatLng | null;
  orderId: string | null;
  tripId: string | null;
  driverId: string | null;
  vehicleType: VehicleType;
  driverPos: LatLng | null;
  /** First driver position seen, so the map fits it once instead of on every update. */
  driverAnchor: LatLng | null;
  /** Who cancelled, once the ride is "cancelled": the rider or the driver. */
  cancelledBy: CancelledBy;
  /** What the rider pays (VND), locked at booking; null until booked. */
  price: number | null;
  surgeMultiplier: number;
}

export const initialRide: RideState = {
  phase: "idle",
  reached: "searching",
  dropoff: null,
  orderId: null,
  tripId: null,
  driverId: null,
  vehicleType: "CAR",
  driverPos: null,
  driverAnchor: null,
  cancelledBy: "",
  price: null,
  surgeMultiplier: 1,
};

/** A ride that was already under way when the page opened. */
export interface ActiveRide {
  orderId: string;
  phase: "searching" | "assigned" | "in_progress";
  vehicleType: VehicleType;
  dropoff: LatLng;
  driverId?: string;
  tripId?: string;
  price?: number | null;
  surgeMultiplier?: number;
}

/** How a search that was still pending turned out (found by asking, not by a push). */
export type Outcome =
  | { kind: "matched"; driverId: string; vehicleType: VehicleType; tripId?: string }
  | { kind: "no_driver" }
  | { kind: "cancelled"; cancelledBy: CancelledBy };

type Action =
  | { type: "setDropoff"; dropoff: LatLng }
  | { type: "setVehicle"; vehicleType: VehicleType }
  | { type: "restored"; ride: ActiveRide }
  | { type: "outcome"; orderId: string; outcome: Outcome }
  | { type: "bookStarted" }
  | { type: "bookSucceeded"; orderId: string; price?: number | null; surgeMultiplier?: number }
  | { type: "bookFailed" }
  | { type: "tripLoaded"; tripId: string }
  | { type: "orderCancelled"; orderId: string }
  | { type: "notification"; message: NotificationMessage }
  | { type: "reset" };

const STEP_OF_PHASE: Partial<Record<Phase, Step>> = {
  booking: "searching",
  searching: "searching",
  assigned: "assigned",
  in_progress: "in_progress",
  completed: "completed",
};

function moveTo(state: RideState, phase: Phase, patch: Partial<RideState> = {}): RideState {
  return { ...state, ...patch, phase, reached: STEP_OF_PHASE[phase] ?? state.reached };
}

const ACTIVE: Phase[] = ["booking", "searching", "assigned", "in_progress"];

function reduce(state: RideState, action: Action): RideState {
  switch (action.type) {
    case "setDropoff":
      return state.phase === "idle" ? { ...state, dropoff: action.dropoff } : state;

    case "setVehicle":
      return state.phase === "idle" ? { ...state, vehicleType: action.vehicleType } : state;

    case "restored": {
      // Only from a fresh page: a push that arrived first knows better.
      if (state.phase !== "idle") return state;
      const { ride } = action;
      return moveTo(state, ride.phase, {
        orderId: ride.orderId,
        vehicleType: ride.vehicleType,
        dropoff: ride.dropoff,
        driverId: ride.driverId ?? null,
        tripId: ride.tripId ?? null,
        price: ride.price ?? null,
        surgeMultiplier: ride.surgeMultiplier ?? 1,
      });
    }

    case "outcome": {
      // Applies only to the search still in progress: the push may have got here first.
      if (state.phase !== "searching" || state.orderId !== action.orderId) return state;
      const { outcome } = action;
      if (outcome.kind === "no_driver") return moveTo(state, "no_driver");
      if (outcome.kind === "cancelled") return moveTo(state, "cancelled", { cancelledBy: outcome.cancelledBy });
      return moveTo(state, "assigned", {
        driverId: outcome.driverId,
        vehicleType: outcome.vehicleType,
        tripId: outcome.tripId ?? state.tripId,
      });
    }

    case "bookStarted":
      return moveTo(state, "booking");

    case "bookSucceeded":
      // The match can be pushed over the WebSocket before the POST response
      // lands, in which case the ride has already moved past "booking".
      const priced = { price: action.price ?? null, surgeMultiplier: action.surgeMultiplier ?? 1 };
      return state.phase === "booking"
        ? moveTo(state, "searching", { orderId: action.orderId, ...priced })
        : { ...state, orderId: state.orderId ?? action.orderId, ...priced };

    case "bookFailed":
      return moveTo(state, "idle");

    case "tripLoaded":
      return { ...state, tripId: action.tripId };

    case "orderCancelled":
      // The rider's own cancel was accepted: show it now rather than waiting for the push.
      return ACTIVE.includes(state.phase) && state.orderId === action.orderId
        ? moveTo(state, "cancelled", { cancelledBy: "RIDER" })
        : state;

    case "reset":
      // Keep the vehicle choice: most riders book the same kind again.
      return { ...initialRide, vehicleType: state.vehicleType };

    case "notification": {
      if (!ACTIVE.includes(state.phase)) return state;
      const { message } = action;

      if (message.type === "driver.location.updated") {
        if (state.driverId && message.payload.driverId !== state.driverId) return state;
        const pos = { lat: message.payload.lat, lng: message.payload.lng };
        return { ...state, driverPos: pos, driverAnchor: state.driverAnchor ?? pos };
      }

      // A rider has one active order at a time, so before the POST response
      // tells us the orderId, the first order-related push is ours.
      if (state.orderId && message.payload.orderId !== state.orderId) return state;
      const orderId = message.payload.orderId;

      switch (message.type) {
        case "order.matched":
          return moveTo(state, "assigned", {
            orderId,
            driverId: message.payload.driverId,
            vehicleType: message.payload.vehicleType,
          });
        case "order.no_driver_found":
          return moveTo(state, "no_driver", { orderId });
        case "order.cancelled":
          return moveTo(state, "cancelled", { orderId, cancelledBy: message.payload.cancelledBy || "RIDER" });
        case "trip.status.changed": {
          const { tripId, driverId, status } = message.payload;
          const patch = { orderId, tripId, driverId };
          if (status === "IN_PROGRESS") return moveTo(state, "in_progress", patch);
          if (status === "COMPLETED") return moveTo(state, "completed", patch);
          if (status === "CANCELLED") return moveTo(state, "cancelled", { ...patch, cancelledBy: message.payload.cancelledBy });
          return moveTo(state, "assigned", patch);
        }
        case "trip.rated":
          // Ratings are only pushed to the driver.
          return state;
      }
    }
  }
}

export function useRide() {
  return useReducer(reduce, initialRide);
}
