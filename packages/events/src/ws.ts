import type {
  DriverLocationUpdated,
  OrderCancelled,
  OrderMatched,
  OrderNoDriverFound,
  TripRated,
  TripStatusChanged,
} from "./types";

/** Everything Notification Service pushes down a rider/driver WebSocket. */
export type NotificationMessage =
  | { type: "order.matched"; payload: OrderMatched }
  | { type: "order.no_driver_found"; payload: OrderNoDriverFound }
  | { type: "order.cancelled"; payload: OrderCancelled }
  | { type: "trip.status.changed"; payload: TripStatusChanged }
  | { type: "driver.location.updated"; payload: DriverLocationUpdated }
  | { type: "trip.rated"; payload: TripRated };
