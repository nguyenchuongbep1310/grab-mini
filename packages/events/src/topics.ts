export const Topics = {
  OrderCreated: "order.created",
  OrderMatched: "order.matched",
  OrderNoDriverFound: "order.no_driver_found",
  DriverLocationUpdated: "driver.location.updated",
  TripStatusChanged: "trip.status.changed",
} as const;

export type Topic = (typeof Topics)[keyof typeof Topics];

export const DeadLetterTopic = (topic: Topic): string => `${topic}.DLQ`;
