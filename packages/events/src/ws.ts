import type {
  ChatMessageSent,
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

/**
 * The chat push Notification Service sends to the other person on a trip. Not yet part of
 * `NotificationMessage`: the app reducers switch on that union without a default, so a new member
 * must be added together with a `case` in each (task 10.3). Until then the apps would not know what to do with it.
 */
export type ChatNotification = { type: "chat.message.sent"; payload: ChatMessageSent };
