import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { Order, RiderPayment, Trip } from "@grab-mini/client";
import { COMPLETED_RIDE_WINDOW_MS, dismissCompletedRide, findActiveRide } from "./recovery";

const NOW = new Date("2026-06-10T12:00:00Z");
const HOUR = 3600_000;

const order = (status: Order["status"], extra: Partial<Order> = {}): Order => ({
  id: "o-1",
  riderId: "rider-1",
  pickup: { lat: 10.77, lng: 106.7 },
  dropoff: { lat: 10.78, lng: 106.71 },
  vehicleType: "CAR",
  status,
  price: 100000,
  promoCode: "WELCOME10",
  discount: 10000,
  ...extra,
});
const trip = (status: Trip["status"], extra: Partial<Trip> = {}): Trip => ({
  id: "t-1",
  orderId: "o-1",
  driverId: "driver-2",
  status,
  ...extra,
});
const fare = (agoMs: number): RiderPayment => ({
  id: "p-1",
  kind: "FARE",
  status: "CAPTURED",
  amount: 90000,
  currency: "VND",
  createdAt: new Date(NOW.getTime() - agoMs).toISOString(),
});
const payments = (...p: RiderPayment[]) => ({ tripId: "t-1", currency: "VND", total: 0, payments: p });

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW);
  localStorage.clear();
  vi.spyOn(client, "getLatestOrder").mockResolvedValue(order("MATCHED"));
  vi.spyOn(client, "waitForTripByOrder").mockResolvedValue(trip("COMPLETED"));
  vi.spyOn(client, "getTripPayments").mockResolvedValue(payments(fare(HOUR)));
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("findActiveRide", () => {
  it("has nothing to restore without an order", async () => {
    vi.mocked(client.getLatestOrder).mockResolvedValue(null);
    expect(await findActiveRide("rider-1")).toBeNull();
  });

  it("restores a pending search", async () => {
    vi.mocked(client.getLatestOrder).mockResolvedValue(order("PENDING"));
    expect(await findActiveRide("rider-1")).toMatchObject({ orderId: "o-1", phase: "searching" });
  });

  it.each([
    ["ASSIGNED", "assigned"],
    ["IN_PROGRESS", "in_progress"],
  ] as const)("restores a %s trip as %s", async (status, phase) => {
    vi.mocked(client.waitForTripByOrder).mockResolvedValue(trip(status));
    expect(await findActiveRide("rider-1")).toMatchObject({ phase, tripId: "t-1", driverId: "driver-2" });
    expect(client.getTripPayments).not.toHaveBeenCalled();
  });

  it("restores a completed, unrated trip that ended within the window, with its price and promo", async () => {
    expect(await findActiveRide("rider-1")).toEqual({
      orderId: "o-1",
      phase: "completed",
      vehicleType: "CAR",
      dropoff: { lat: 10.78, lng: 106.71 },
      price: 100000,
      surgeMultiplier: undefined,
      promoCode: "WELCOME10",
      discount: 10000,
      driverId: "driver-2",
      tripId: "t-1",
    });
  });

  it("restores a completed trip Payment Service has not settled yet", async () => {
    vi.mocked(client.getTripPayments).mockResolvedValue(payments());
    expect(await findActiveRide("rider-1")).toMatchObject({ phase: "completed" });
  });

  it("restores just inside the window and not just past it", async () => {
    vi.mocked(client.getTripPayments).mockResolvedValue(payments(fare(COMPLETED_RIDE_WINDOW_MS)));
    expect(await findActiveRide("rider-1")).toMatchObject({ phase: "completed" });
    vi.mocked(client.getTripPayments).mockResolvedValue(payments(fare(COMPLETED_RIDE_WINDOW_MS + 1000)));
    expect(await findActiveRide("rider-1")).toBeNull();
  });

  it("is idle for a completed trip that ended before the window", async () => {
    vi.mocked(client.getTripPayments).mockResolvedValue(payments(fare(3 * 24 * HOUR)));
    expect(await findActiveRide("rider-1")).toBeNull();
  });

  it("is idle for a completed trip that was rated", async () => {
    vi.mocked(client.waitForTripByOrder).mockResolvedValue(trip("COMPLETED", { rating: 5 }));
    expect(await findActiveRide("rider-1")).toBeNull();
    expect(client.getTripPayments).not.toHaveBeenCalled();
  });

  it("is idle for a completed trip the rider already closed, and only for that rider", async () => {
    dismissCompletedRide("rider-1", "t-1");
    expect(await findActiveRide("rider-1")).toBeNull();
    dismissCompletedRide("rider-2", "t-9");
    expect(await findActiveRide("rider-2")).toMatchObject({ phase: "completed" });
  });

  it("gives the same answer on two quick reloads", async () => {
    const [a, b] = await Promise.all([findActiveRide("rider-1"), findActiveRide("rider-1")]);
    expect(a).toMatchObject({ phase: "completed" });
    expect(b).toEqual(a);
  });

  it("is idle for a cancelled trip", async () => {
    vi.mocked(client.waitForTripByOrder).mockResolvedValue(trip("CANCELLED", { cancelledBy: "DRIVER" }));
    expect(await findActiveRide("rider-1")).toBeNull();
  });

  it.each(["NO_DRIVER", "CANCELLED"] as const)("is idle for a %s order", async (status) => {
    vi.mocked(client.getLatestOrder).mockResolvedValue(order(status));
    expect(await findActiveRide("rider-1")).toBeNull();
    expect(client.waitForTripByOrder).not.toHaveBeenCalled();
  });

  it("is idle when the trip can't be found", async () => {
    vi.mocked(client.waitForTripByOrder).mockRejectedValue(new client.ApiError("not found", 404));
    expect(await findActiveRide("rider-1")).toBeNull();
  });

  it("is idle when the age of a completed trip can't be read", async () => {
    vi.mocked(client.getTripPayments).mockRejectedValue(new Error("payment down"));
    expect(await findActiveRide("rider-1")).toBeNull();
  });

  it("still restores when storage is unavailable", async () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("denied");
    });
    expect(await findActiveRide("rider-1")).toMatchObject({ phase: "completed" });
  });

  it("lets the latest-order lookup error through for the caller to ignore", async () => {
    vi.mocked(client.getLatestOrder).mockRejectedValue(new Error("down"));
    await expect(findActiveRide("rider-1")).rejects.toThrow("down");
  });
});

describe("dismissCompletedRide", () => {
  it("does not throw when storage is unavailable", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("quota");
    });
    expect(() => dismissCompletedRide("rider-1", "t-1")).not.toThrow();
  });
});
