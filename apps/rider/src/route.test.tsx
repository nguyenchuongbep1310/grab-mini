import "@testing-library/jest-dom/vitest";
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { LatLng, Route } from "@grab-mini/client";
import { useEta, useRoute } from "./useRoute";

const route = (durationSec: number): Route => ({
  distanceKm: 3.2,
  durationSec,
  polyline: [],
  vehicle: "CAR",
  model: "detour-1.3",
});

const PICKUP: LatLng = { lat: 10.776, lng: 106.7 };
const DROPOFF: LatLng = { lat: 10.79, lng: 106.71 };

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("useRoute", () => {
  it("waits for the debounce, asks once for the latest trip, and exposes the route", async () => {
    const spy = vi.spyOn(client, "getRoute").mockResolvedValue(route(480));
    const { result, rerender } = renderHook(({ to }) => useRoute(PICKUP, to, "CAR", true), {
      initialProps: { to: DROPOFF },
    });

    expect(result.current).toBeNull();
    await act(() => vi.advanceTimersByTimeAsync(200));
    expect(spy).not.toHaveBeenCalled();

    // The rider moves the pin again: the first wait is dropped, not answered.
    const moved = { lat: 10.8, lng: 106.72 };
    rerender({ to: moved });
    await act(() => vi.advanceTimersByTimeAsync(200));
    expect(spy).not.toHaveBeenCalled();
    await act(() => vi.advanceTimersByTimeAsync(150));
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy).toHaveBeenCalledWith(PICKUP, moved, "CAR");
    expect(result.current).toEqual(route(480));
  });

  it("drops the old route when the trip changes, and does nothing without a destination", async () => {
    const spy = vi.spyOn(client, "getRoute").mockResolvedValue(route(480));
    const { result, rerender } = renderHook(({ to, on }) => useRoute(PICKUP, to, "CAR", on), {
      initialProps: { to: DROPOFF as LatLng | null, on: true },
    });
    await act(() => vi.advanceTimersByTimeAsync(300));
    expect(result.current).not.toBeNull();

    rerender({ to: null, on: true });
    expect(result.current).toBeNull();
    rerender({ to: DROPOFF, on: false });
    await act(() => vi.advanceTimersByTimeAsync(1000));
    expect(spy).toHaveBeenCalledTimes(1);
    expect(result.current).toBeNull();
  });

  it("stays null when Routing fails, without throwing", async () => {
    vi.spyOn(client, "getRoute").mockRejectedValue(new Error("routing down"));
    const { result } = renderHook(() => useRoute(PICKUP, DROPOFF, "CAR", true));
    await act(() => vi.advanceTimersByTimeAsync(300));
    expect(result.current).toBeNull();
  });
});

describe("useEta", () => {
  const at = (n: number): LatLng => ({ lat: 10.77 + n * 0.0001, lng: 106.7 });

  it("looks up at once, then at most once per interval using the newest position", async () => {
    const spy = vi.spyOn(client, "getRoute").mockImplementation(async () => route(300));
    const { result, rerender } = renderHook(({ from }) => useEta(from, PICKUP, "CAR", true, 10_000), {
      initialProps: { from: at(0) as LatLng | null },
    });
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(spy).toHaveBeenCalledTimes(1);
    expect(result.current).toEqual(route(300));

    // Locations arrive every 3 s; nothing more goes out before 10 s have passed.
    for (const n of [1, 2, 3]) {
      rerender({ from: at(n) });
      await act(() => vi.advanceTimersByTimeAsync(3000));
    }
    expect(spy).toHaveBeenCalledTimes(1);

    // 10 s after the first lookup, one more, with the newest position.
    await act(() => vi.advanceTimersByTimeAsync(1000));
    expect(spy).toHaveBeenCalledTimes(2);
    expect(spy).toHaveBeenLastCalledWith(at(3), PICKUP, "CAR");

    // Quiet until a new position arrives, then it waits out the rest of the interval.
    await act(() => vi.advanceTimersByTimeAsync(20_000));
    expect(spy).toHaveBeenCalledTimes(2);
    rerender({ from: at(4) });
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(spy).toHaveBeenCalledTimes(3);
  });

  it("does not ask before the driver has a position, or when disabled", async () => {
    const spy = vi.spyOn(client, "getRoute").mockResolvedValue(route(300));
    const { result, rerender } = renderHook(({ from, on }) => useEta(from, PICKUP, "CAR", on), {
      initialProps: { from: null as LatLng | null, on: true },
    });
    await act(() => vi.advanceTimersByTimeAsync(30_000));
    rerender({ from: at(0), on: false });
    await act(() => vi.advanceTimersByTimeAsync(30_000));
    expect(spy).not.toHaveBeenCalled();
    expect(result.current).toBeNull();
  });

  it("switches to the new target when the trip starts and shows nothing after a failure", async () => {
    const spy = vi.spyOn(client, "getRoute").mockResolvedValueOnce(route(300));
    const { result, rerender } = renderHook(({ to }) => useEta(at(0), to, "CAR", true, 10_000), {
      initialProps: { to: PICKUP },
    });
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(result.current).toEqual(route(300));

    // Pickup -> dropoff: the old estimate is for the wrong place, so it goes at once.
    spy.mockRejectedValueOnce(new Error("routing down"));
    rerender({ to: DROPOFF });
    expect(result.current).toBeNull();
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(spy).toHaveBeenLastCalledWith(at(0), DROPOFF, "CAR");
    expect(result.current).toBeNull();
  });
});
