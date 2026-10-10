export const Topics = {
  OrderCreated: "order.created",
  OrderMatched: "order.matched",
  OrderNoDriverFound: "order.no_driver_found",
  OrderCancelled: "order.cancelled",
  DriverLocationUpdated: "driver.location.updated",
  TripStatusChanged: "trip.status.changed",
  TripRated: "trip.rated",
} as const;

export type Topic = (typeof Topics)[keyof typeof Topics];

export const DeadLetterTopic = (topic: Topic): string => `${topic}.DLQ`;
