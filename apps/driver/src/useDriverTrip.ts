import { useReducer } from "react";
import type { CancelledBy, NotificationMessage, TripStatus } from "@grab-mini/events";
import type { LatLng, Order } from "@grab-mini/client";

export type Phase = "idle" | "assigned" | "in_progress" | "completed" | "cancelled";

export interface DriverState {
  phase: Phase;
  orderId: string | null;
  tripId: string | null;
  order: Order | null;
  /** Latest position echoed back from Driver Location Service's input topic. */
  pos: LatLng | null;
  /** First position seen for this trip, so the map fits it once, not on every update. */
  anchor: LatLng | null;
  /** Once "cancelled": who cancelled (the rider or this driver) and the reason given. */
  cancelledBy: CancelledBy;
  cancelReason: string;
}

export const initialDriver: DriverState = {
  phase: "idle",
  orderId: null,
  tripId: null,
  order: null,
  pos: null,
  anchor: null,
  cancelledBy: "",
  cancelReason: "",
};

type Action =
  | { type: "notification"; message: NotificationMessage; driverId: string }
  | { type: "tripLoaded"; tripId: string }
  | { type: "restored"; tripId: string; orderId: string; status: TripStatus }
  | { type: "orderLoaded"; order: Order }
  | { type: "reset" };

function reduce(state: DriverState, action: Action): DriverState {
  switch (action.type) {
    case "tripLoaded":
      return { ...state, tripId: action.tripId };
    case "restored": {
      // The app was closed or reloaded mid-trip. Ignore the answer if a push
      // already told us about a trip while the request was in flight.
      if (state.phase !== "idle") return state;
      const phase = action.status === "IN_PROGRESS" ? "in_progress" : "assigned";
      return { ...state, phase, orderId: action.orderId, tripId: action.tripId, anchor: state.pos };
    }
    case "orderLoaded":
      return { ...state, order: action.order };
    case "reset":
      // Keep the live position: the driver is still out there.
      return { ...initialDriver, pos: state.pos };

    case "notification": {
      const { message, driverId } = action;
      switch (message.type) {
        case "driver.location.updated": {
          if (message.payload.driverId !== driverId) return state;
          const pos = { lat: message.payload.lat, lng: message.payload.lng };
          const tracking = state.phase === "assigned" || state.phase === "in_progress";
          return { ...state, pos, anchor: tracking ? (state.anchor ?? pos) : state.anchor };
        }
        case "order.matched":
          if (message.payload.driverId !== driverId || state.phase !== "idle") return state;
          return { ...initialDriver, phase: "assigned", orderId: message.payload.orderId, pos: state.pos, anchor: state.pos };
        case "trip.status.changed": {
          if (message.payload.driverId !== driverId) return state;
          if (state.orderId && message.payload.orderId !== state.orderId) return state;
          const { status, tripId, orderId } = message.payload;
          const patch = { tripId, orderId };
          if (status === "IN_PROGRESS") return { ...state, ...patch, phase: "in_progress" };
          if (status === "COMPLETED") return { ...state, ...patch, phase: "completed" };
          if (status === "CANCELLED") {
            const { cancelledBy, reason } = message.payload;
            return { ...state, ...patch, phase: "cancelled", cancelledBy, cancelReason: reason };
          }
          return { ...state, ...patch };
        }
        case "order.cancelled": {
          // The rider cancelled an order this driver was matched to; the trip's
          // own cancelled event follows and fills in the same state.
          const { orderId, cancelledBy, reason } = message.payload;
          const working = state.phase === "assigned" || state.phase === "in_progress";
          if (!working || state.orderId !== orderId) return state;
          return { ...state, phase: "cancelled", cancelledBy: cancelledBy || "RIDER", cancelReason: reason };
        }
        case "order.no_driver_found":
        case "trip.rated":
          // Not part of the trip flow (the app reacts to ratings separately).
          return state;
      }
    }
  }
}

export function useDriverTrip() {
  return useReducer(reduce, initialDriver);
}
