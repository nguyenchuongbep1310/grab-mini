/** Base fields every event on the bus carries, for idempotency and tracing (see US-6.1). */
export interface EventEnvelope {
  eventId: string;
  occurredAt: string;
}

export type OrderStatus = "PENDING" | "MATCHED" | "NO_DRIVER";
export type TripStatus = "ASSIGNED" | "IN_PROGRESS" | "COMPLETED" | "CANCELLED";
export type DriverStatus = "AVAILABLE" | "ASSIGNED" | "ON_TRIP";

export interface OrderCreated extends EventEnvelope {
  orderId: string;
  riderId: string;
  pickup: { lat: number; lng: number };
  dropoff: { lat: number; lng: number };
}

export interface OrderMatched extends EventEnvelope {
  orderId: string;
  driverId: string;
}

export interface OrderNoDriverFound extends EventEnvelope {
  orderId: string;
}

export interface DriverLocationUpdated extends EventEnvelope {
  driverId: string;
  lat: number;
  lng: number;
  /** Source timestamp from the driver device, used to discard out-of-order updates (US-3.1). */
  timestamp: string;
}

export interface TripStatusChanged extends EventEnvelope {
  tripId: string;
  orderId: string;
  driverId: string;
  status: TripStatus;
}
