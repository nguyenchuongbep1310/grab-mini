import { useReducer } from "react";
import type { NotificationMessage } from "@grab-mini/events";
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
}

export const initialDriver: DriverState = {
  phase: "idle",
  orderId: null,
  tripId: null,
  order: null,
  pos: null,
  anchor: null,
};

type Action =
  | { type: "notification"; message: NotificationMessage; driverId: string }
  | { type: "tripLoaded"; tripId: string }
  | { type: "orderLoaded"; order: Order }
  | { type: "reset" };

function reduce(state: DriverState, action: Action): DriverState {
  switch (action.type) {
    case "tripLoaded":
      return { ...state, tripId: action.tripId };
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
          if (status === "CANCELLED") return { ...state, ...patch, phase: "cancelled" };
          return { ...state, ...patch };
        }
        case "order.no_driver_found":
          return state;
      }
    }
  }
}

export function useDriverTrip() {
  return useReducer(reduce, initialDriver);
}
