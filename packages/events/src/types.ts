/** Base fields every event on the bus carries, for idempotency and tracing (see US-6.1). */
export interface EventEnvelope {
  eventId: string;
  occurredAt: string;
}

export type OrderStatus = "PENDING" | "MATCHED" | "NO_DRIVER" | "CANCELLED";
/** Who cancelled a ride. Empty string on events from before cancellations were recorded. */
export type CancelledBy = "RIDER" | "DRIVER" | "";
export type TripStatus = "ASSIGNED" | "IN_PROGRESS" | "COMPLETED" | "CANCELLED";
export type VehicleType = "CAR" | "BIKE";
export type DriverStatus = "AVAILABLE" | "ASSIGNED" | "ON_TRIP";

export interface OrderCreated extends EventEnvelope {
  orderId: string;
  riderId: string;
  pickup: { lat: number; lng: number };
  dropoff: { lat: number; lng: number };
  vehicleType: VehicleType;
  /** Surge multiplier the order was booked at (schema v3); 1 when absent. */
  surgeMultiplier?: number;
  /** Price locked at booking, in VND (schema v3); null/absent on older messages. */
  price?: number | null;
}

export interface OrderMatched extends EventEnvelope {
  orderId: string;
  driverId: string;
  vehicleType: VehicleType;
  /** The rider who made the order. Empty on messages from before this field existed. */
  riderId: string;
}

export interface OrderNoDriverFound extends EventEnvelope {
  orderId: string;
  /** Empty on messages from before this field existed. */
  riderId: string;
}

/** The rider cancelled an order that had no driver yet (or none known to them). */
export interface OrderCancelled extends EventEnvelope {
  orderId: string;
  riderId: string;
  cancelledBy: CancelledBy;
  /** What the rider said, or empty. */
  reason: string;
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
  /** Empty on messages from before this field existed. */
  riderId: string;
  /** Set when status is CANCELLED; empty otherwise and on older messages. */
  cancelledBy: CancelledBy;
  reason: string;
}

export interface TripRated extends EventEnvelope {
  tripId: string;
  orderId: string;
  riderId: string;
  driverId: string;
  /** 1 to 5. */
  stars: number;
  comment: string;
  /** Recorded tip in whole VND; 0 or absent when there is none. */
  tipAmount?: number;
  currency?: string;
}
