import { useReducer } from "react";
import type { NotificationMessage } from "@grab-mini/events";
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
  driverPos: LatLng | null;
  /** First driver position seen, so the map fits it once instead of on every update. */
  driverAnchor: LatLng | null;
}

export const initialRide: RideState = {
  phase: "idle",
  reached: "searching",
  dropoff: null,
  orderId: null,
  tripId: null,
  driverId: null,
  driverPos: null,
  driverAnchor: null,
};

type Action =
  | { type: "setDropoff"; dropoff: LatLng }
  | { type: "bookStarted" }
  | { type: "bookSucceeded"; orderId: string }
  | { type: "bookFailed" }
  | { type: "tripLoaded"; tripId: string }
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

    case "bookStarted":
      return moveTo(state, "booking");

    case "bookSucceeded":
      // The match can be pushed over the WebSocket before the POST response
      // lands, in which case the ride has already moved past "booking".
      return state.phase === "booking"
        ? moveTo(state, "searching", { orderId: action.orderId })
        : { ...state, orderId: state.orderId ?? action.orderId };

    case "bookFailed":
      return moveTo(state, "idle");

    case "tripLoaded":
      return { ...state, tripId: action.tripId };

    case "reset":
      return initialRide;

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
          return moveTo(state, "assigned", { orderId, driverId: message.payload.driverId });
        case "order.no_driver_found":
          return moveTo(state, "no_driver", { orderId });
        case "trip.status.changed": {
          const { tripId, driverId, status } = message.payload;
          const patch = { orderId, tripId, driverId };
          if (status === "IN_PROGRESS") return moveTo(state, "in_progress", patch);
          if (status === "COMPLETED") return moveTo(state, "completed", patch);
          if (status === "CANCELLED") return moveTo(state, "cancelled", patch);
          return moveTo(state, "assigned", patch);
        }
      }
    }
  }
}

export function useRide() {
  return useReducer(reduce, initialRide);
}
