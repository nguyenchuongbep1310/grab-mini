import "@testing-library/jest-dom/vitest";
import { render, renderHook, screen, fireEvent, act, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useRide } from "./useRide";
import { useQuote } from "./useQuote";
import { checkOutcome, findActiveRide } from "./recovery";
import { RatingSummary, TipPicker } from "./TipPicker";
import * as client from "@grab-mini/client";

describe("useRide state machine", () => {
  it("runs through idle -> booking -> searching -> assigned -> in_progress -> completed -> reset", () => {
    const { result } = renderHook(() => useRide());

    expect(result.current[0].phase).toBe("idle");
    expect(result.current[0].vehicleType).toBe("CAR");

    act(() => {
      result.current[1]({ type: "setVehicle", vehicleType: "BIKE" });
      result.current[1]({ type: "setDropoff", dropoff: { lat: 10.78, lng: 106.71 } });
    });
    expect(result.current[0].vehicleType).toBe("BIKE");
    expect(result.current[0].dropoff).toEqual({ lat: 10.78, lng: 106.71 });

    act(() => {
      result.current[1]({ type: "bookStarted" });
      result.current[1]({ type: "bookSucceeded", orderId: "order-1", price: 32000, surgeMultiplier: 1.2 });
    });
    expect(result.current[0].phase).toBe("searching");
    expect(result.current[0].orderId).toBe("order-1");
    expect(result.current[0].price).toBe(32000);
    expect(result.current[0].surgeMultiplier).toBe(1.2);

    // Match notification arrives
    act(() => {
      result.current[1]({
        type: "notification",
        message: {
          type: "order.matched",
          payload: {
            eventId: "e-1",
            orderId: "order-1",
            driverId: "driver-4",
            riderId: "rider-1",
            vehicleType: "BIKE",
            occurredAt: "2026-01-01T00:00:00Z",
          },
        },
      });
      result.current[1]({ type: "tripLoaded", tripId: "trip-1" });
    });
    expect(result.current[0].phase).toBe("assigned");
    expect(result.current[0].driverId).toBe("driver-4");
    expect(result.current[0].tripId).toBe("trip-1");

    // Driver location update sets driverPos and locks driverAnchor on the first update
    act(() => {
      result.current[1]({
        type: "notification",
        message: {
          type: "driver.location.updated",
          payload: {
            eventId: "e-2",
            occurredAt: "2026-01-01T00:00:01Z",
            driverId: "driver-4",
            lat: 10.775,
            lng: 106.701,
            timestamp: "2026-01-01T00:00:01Z",
          },
        },
      });
      result.current[1]({
        type: "notification",
        message: {
          type: "driver.location.updated",
          payload: {
            eventId: "e-3",
            occurredAt: "2026-01-01T00:00:04Z",
            driverId: "driver-4",
            lat: 10.776,
            lng: 106.702,
            timestamp: "2026-01-01T00:00:04Z",
          },
        },
      });
    });
    expect(result.current[0].driverAnchor).toEqual({ lat: 10.775, lng: 106.701 });
    expect(result.current[0].driverPos).toEqual({ lat: 10.776, lng: 106.702 });

    // Pickup -> IN_PROGRESS
    act(() => {
      result.current[1]({
        type: "notification",
        message: {
          type: "trip.status.changed",
          payload: {
            eventId: "e-4",
            tripId: "trip-1",
            orderId: "order-1",
            driverId: "driver-4",
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
    expect(result.current[0].reached).toBe("in_progress");

    // Complete -> COMPLETED
    act(() => {
      result.current[1]({
        type: "notification",
        message: {
          type: "trip.status.changed",
          payload: {
            eventId: "e-5",
            tripId: "trip-1",
            orderId: "order-1",
            driverId: "driver-4",
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
    expect(result.current[0].reached).toBe("completed");

    // Reset keeps vehicle choice
    act(() => {
      result.current[1]({ type: "reset" });
    });
    expect(result.current[0].phase).toBe("idle");
    expect(result.current[0].vehicleType).toBe("BIKE");
  });

  it("handles WebSocket match arriving before HTTP bookSucceeded", () => {
    const { result } = renderHook(() => useRide());

    act(() => {
      result.current[1]({ type: "bookStarted" });
      result.current[1]({
        type: "notification",
        message: {
          type: "order.matched",
          payload: {
            eventId: "e-fast",
            orderId: "order-fast",
            driverId: "driver-1",
            riderId: "rider-1",
            vehicleType: "CAR",
            occurredAt: "2026-01-01T00:00:00Z",
          },
        },
      });
      result.current[1]({ type: "bookSucceeded", orderId: "order-fast", price: 50000, surgeMultiplier: 1.0 });
    });

    expect(result.current[0].phase).toBe("assigned");
    expect(result.current[0].orderId).toBe("order-fast");
    expect(result.current[0].price).toBe(50000);
  });

  it("handles cancellations and no_driver outcomes", () => {
    const { result } = renderHook(() => useRide());

    act(() => {
      result.current[1]({ type: "bookStarted" });
      result.current[1]({ type: "bookSucceeded", orderId: "order-2" });
      result.current[1]({ type: "outcome", orderId: "order-2", outcome: { kind: "no_driver" } });
    });
    expect(result.current[0].phase).toBe("no_driver");

    act(() => {
      result.current[1]({ type: "reset" });
      result.current[1]({
        type: "restored",
        ride: {
          orderId: "order-3",
          phase: "assigned",
          vehicleType: "CAR",
          dropoff: { lat: 10.79, lng: 106.72 },
          driverId: "driver-2",
          tripId: "trip-3",
        },
      });
      result.current[1]({
        type: "notification",
        message: {
          type: "trip.status.changed",
          payload: {
            eventId: "e-cancel",
            tripId: "trip-3",
            orderId: "order-3",
            driverId: "driver-2",
            riderId: "rider-1",
            status: "CANCELLED",
            occurredAt: "2026-01-01T00:02:00Z",
            cancelledBy: "DRIVER",
            reason: "Vehicle problem",
          },
        },
      });
    });

    expect(result.current[0].phase).toBe("cancelled");
    expect(result.current[0].cancelledBy).toBe("DRIVER");
    expect(result.current[0].reached).toBe("assigned");
  });
});

describe("useQuote hook", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("debounces getQuote, exposes ready quote, and supports applyNewQuote / acknowledge", async () => {
    const q1: client.Quote = {
      quoteId: "q-1",
      baseFare: 30000,
      multiplier: 1.0,
      price: 30000,
      currency: "VND",
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    };
    const q2: client.Quote = { ...q1, quoteId: "q-2", multiplier: 1.4, price: 42000 };
    vi.spyOn(client, "getQuote").mockResolvedValue(q1);

    const { result } = renderHook(() =>
      useQuote({ lat: 10.776, lng: 106.7 }, { lat: 10.79, lng: 106.71 }, "CAR", true),
    );

    expect(result.current.status).toBe("loading");
    await waitFor(() => expect(result.current.status).toBe("ready"));
    expect(result.current.quote).toEqual(q1);
    expect(result.current.changed).toBe(false);

    act(() => {
      result.current.applyNewQuote(q2);
    });
    expect(result.current.quote).toEqual(q2);
    expect(result.current.changed).toBe(true);

    act(() => {
      result.current.acknowledge();
    });
    expect(result.current.changed).toBe(false);
  });
});

describe("recovery helpers (findActiveRide & checkOutcome)", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("restores searching or in-progress ride from latest order and trip", async () => {
    vi.spyOn(client, "getLatestOrder").mockResolvedValueOnce({
      id: "o-pending",
      riderId: "rider-1",
      pickup: { lat: 10.77, lng: 106.7 },
      dropoff: { lat: 10.78, lng: 106.71 },
      vehicleType: "BIKE",
      status: "PENDING",
      price: 25000,
      surgeMultiplier: 1.0,
    });
    const r1 = await findActiveRide("rider-1");
    expect(r1).toMatchObject({ orderId: "o-pending", phase: "searching", vehicleType: "BIKE" });

    vi.spyOn(client, "getLatestOrder").mockResolvedValueOnce({
      id: "o-matched",
      riderId: "rider-1",
      pickup: { lat: 10.77, lng: 106.7 },
      dropoff: { lat: 10.78, lng: 106.71 },
      vehicleType: "CAR",
      status: "MATCHED",
    });
    vi.spyOn(client, "waitForTripByOrder").mockResolvedValueOnce({
      id: "t-1",
      orderId: "o-matched",
      driverId: "driver-2",
      status: "IN_PROGRESS",
    });
    const r2 = await findActiveRide("rider-1");
    expect(r2).toMatchObject({ orderId: "o-matched", phase: "in_progress", driverId: "driver-2", tripId: "t-1" });
  });

  it("checks outcome of a pending search (no_driver, cancelled, matched)", async () => {
    vi.spyOn(client, "getOrder").mockResolvedValueOnce({
      id: "o-1",
      riderId: "r-1",
      pickup: { lat: 10, lng: 106 },
      dropoff: { lat: 10.1, lng: 106.1 },
      vehicleType: "CAR",
      status: "NO_DRIVER",
    });
    expect(await checkOutcome("o-1")).toEqual({ kind: "no_driver" });

    vi.spyOn(client, "getOrder").mockResolvedValueOnce({
      id: "o-2",
      riderId: "r-1",
      pickup: { lat: 10, lng: 106 },
      dropoff: { lat: 10.1, lng: 106.1 },
      vehicleType: "CAR",
      status: "CANCELLED",
      cancelledBy: "RIDER",
    });
    expect(await checkOutcome("o-2")).toEqual({ kind: "cancelled", cancelledBy: "RIDER" });
  });
});

describe("TipPicker & RatingSummary", () => {
  it("allows preset and custom tips and renders RatingSummary", () => {
    const onComment = vi.fn();
    const onTip = vi.fn();
    const { rerender } = render(<TipPicker comment="" onComment={onComment} tip={0} onTip={onTip} />);

    fireEvent.click(screen.getByRole("button", { name: /20\.000/ }));
    expect(onTip).toHaveBeenCalledWith(20_000);

    fireEvent.click(screen.getByRole("button", { name: "Custom" }));
    const input = screen.getByLabelText("Custom tip in VND");
    fireEvent.change(input, { target: { value: "35000" } });
    expect(onTip).toHaveBeenLastCalledWith(35_000);

    rerender(<RatingSummary comment="  Very polite driver  " tip={20_000} />);
    expect(screen.getByText(/20\.000/)).toBeInTheDocument();
    expect(screen.getByText("Very polite driver")).toBeInTheDocument();
  });
});
