import { renderHook, act } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useDriverTrip } from "./useDriverTrip";

describe("useDriverTrip state machine", () => {
  it("transitions through idle -> assigned -> in_progress -> completed -> reset while keeping live position", () => {
    const { result } = renderHook(() => useDriverTrip());

    expect(result.current[0].phase).toBe("idle");

    // Location update while idle updates pos, not anchor
    act(() => {
      result.current[1]({
        type: "notification",
        driverId: "driver-1",
        message: {
          type: "driver.location.updated",
          payload: {
            eventId: "e-1",
            occurredAt: "2026-01-01T00:00:00Z",
            driverId: "driver-1",
            lat: 10.776,
            lng: 106.7,
            timestamp: "2026-01-01T00:00:00Z",
          },
        },
      });
    });
    expect(result.current[0].pos).toEqual({ lat: 10.776, lng: 106.7 });
    expect(result.current[0].anchor).toBeNull();

    // Match notification for another driver is ignored
    act(() => {
      result.current[1]({
        type: "notification",
        driverId: "driver-1",
        message: {
          type: "order.matched",
          payload: {
            eventId: "e-2",
            orderId: "order-other",
            driverId: "driver-2",
            riderId: "rider-2",
            vehicleType: "CAR",
            occurredAt: "2026-01-01T00:00:01Z",
          },
        },
      });
    });
    expect(result.current[0].phase).toBe("idle");

    // Match notification for driver-1 moves to assigned
    act(() => {
      result.current[1]({
        type: "notification",
        driverId: "driver-1",
        message: {
          type: "order.matched",
          payload: {
            eventId: "e-3",
            orderId: "order-1",
            driverId: "driver-1",
            riderId: "rider-1",
            vehicleType: "CAR",
            occurredAt: "2026-01-01T00:00:02Z",
          },
        },
      });
      result.current[1]({ type: "tripLoaded", tripId: "trip-1" });
    });
    expect(result.current[0].phase).toBe("assigned");
    expect(result.current[0].orderId).toBe("order-1");
    expect(result.current[0].tripId).toBe("trip-1");
    expect(result.current[0].anchor).toEqual({ lat: 10.776, lng: 106.7 });

    // Pickup -> IN_PROGRESS -> COMPLETED
    act(() => {
      result.current[1]({
        type: "notification",
        driverId: "driver-1",
        message: {
          type: "trip.status.changed",
          payload: {
            eventId: "e-4",
            tripId: "trip-1",
            orderId: "order-1",
            driverId: "driver-1",
            riderId: "rider-1",
            status: "IN_PROGRESS",
            occurredAt: "2026-01-01T00:01:00Z",
            cancelledBy: "",
            reason: "",
          },
        },
      });
    });
    expect(result.current[0].phase).toBe("in_progress");

    act(() => {
      result.current[1]({
        type: "notification",
        driverId: "driver-1",
        message: {
          type: "trip.status.changed",
          payload: {
            eventId: "e-5",
            tripId: "trip-1",
            orderId: "order-1",
            driverId: "driver-1",
            riderId: "rider-1",
            status: "COMPLETED",
            occurredAt: "2026-01-01T00:05:00Z",
            cancelledBy: "",
            reason: "",
          },
        },
      });
    });
    expect(result.current[0].phase).toBe("completed");

    // Reset returns to idle and preserves pos
    act(() => {
      result.current[1]({ type: "reset" });
    });
    expect(result.current[0].phase).toBe("idle");
    expect(result.current[0].pos).toEqual({ lat: 10.776, lng: 106.7 });
  });

  it("handles rider cancellation via order.cancelled and trip.status.changed", () => {
    const { result } = renderHook(() => useDriverTrip());

    act(() => {
      result.current[1]({
        type: "restored",
        tripId: "trip-2",
        orderId: "order-2",
        status: "ASSIGNED",
      });
      result.current[1]({
        type: "notification",
        driverId: "driver-1",
        message: {
          type: "order.cancelled",
          payload: {
            eventId: "e-6",
            orderId: "order-2",
            riderId: "rider-1",
            cancelledBy: "RIDER",
            reason: "Changed my plans",
            occurredAt: "2026-01-01T00:01:00Z",
          },
        },
      });
    });

    expect(result.current[0].phase).toBe("cancelled");
    expect(result.current[0].cancelledBy).toBe("RIDER");
    expect(result.current[0].cancelReason).toBe("Changed my plans");
  });
});
