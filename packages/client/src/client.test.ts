import { renderHook, act, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { forgetToken, getToken, isSignedIn, signIn } from "./auth";
import {
  ApiError,
  cancelOrder,
  createOrder,
  createPromotion,
  disablePromotion,
  getActiveTrip,
  getDriverRating,
  getDriverReviews,
  getLatestOrder,
  getOrder,
  getQuote,
  getRoute,
  listPromotions,
  newQuoteOf,
  promoBookingErrorOf,
  promoRejectionOf,
  rateTrip,
  tripAction,
  useNotificationSocket,
  validatePromotion,
  waitForOrder,
  waitForTripByOrder,
} from "./index";

describe("auth & api client", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    vi.stubGlobal("fetch", fetchMock);
    fetchMock.mockReset();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("fetches and caches token, and deduplicates concurrent getToken calls", async () => {
    signIn("rider", "rider-1");
    expect(isSignedIn()).toBe(true);

    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ token: "jwt-123", expiresAt: future }), { status: 200 }),
    );

    const [t1, t2] = await Promise.all([getToken(), getToken()]);
    expect(t1).toBe("jwt-123");
    expect(t2).toBe("jwt-123");
    expect(fetchMock).toHaveBeenCalledTimes(1);

    // Cached call does not hit fetch again
    const t3 = await getToken();
    expect(t3).toBe("jwt-123");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("retries once on 401 by refreshing the token", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();

    fetchMock
      // First token fetch
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "expired-jwt", expiresAt: future }), { status: 200 }))
      // Order GET returns 401
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "unauthorized" }), { status: 401 }))
      // Refresh token fetch
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "fresh-jwt", expiresAt: future }), { status: 200 }))
      // Order GET succeeds
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "o-1", riderId: "rider-1", status: "PENDING" }), { status: 200 }),
      );

    const order = await getOrder("o-1");
    expect(order.id).toBe("o-1");
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it("extracts newQuote from 409 conflict on createOrder", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    const newQuote = {
      quoteId: "q-2",
      baseFare: 30000,
      multiplier: 1.5,
      price: 45000,
      currency: "VND",
      expiresAt: future,
    };

    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "the price has changed", newQuote }), { status: 409 }),
      );

    let caught: unknown;
    try {
      await createOrder("rider-1", { lat: 10.77, lng: 106.7 }, { lat: 10.78, lng: 106.71 }, "CAR", "q-1");
    } catch (err) {
      caught = err;
    }

    expect(caught).toBeInstanceOf(ApiError);
    expect(newQuoteOf(caught)).toEqual(newQuote);
    expect(newQuoteOf(new Error("other"))).toBeNull();
  });

  it("sends a fresh Idempotency-Key per booking and reuses it when the attempt is retried", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockRejectedValueOnce(new TypeError("network down"))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "boom" }), { status: 503 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ orderId: "o-1" }), { status: 201 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ orderId: "o-2" }), { status: 201 }));

    const pickup = { lat: 10.77, lng: 106.7 };
    const dropoff = { lat: 10.78, lng: 106.71 };
    await expect(createOrder("rider-1", pickup, dropoff, "CAR", "q-1", 0)).resolves.toEqual({ orderId: "o-1" });
    await createOrder("rider-1", pickup, dropoff, "CAR", undefined, 0);

    const keys = fetchMock.mock.calls
      .filter(([url]) => url === "/order-api/orders")
      .map(([, init]) => new Headers(init.headers).get("Idempotency-Key"));
    expect(keys).toHaveLength(4);
    expect(keys[0]).toBeTruthy();
    expect(keys[1]).toBe(keys[0]); // retried after the network error
    expect(keys[2]).toBe(keys[0]); // and after the 503
    expect(keys[3]).not.toBe(keys[0]); // a new booking gets a new key
  });

  it("does not retry a 4xx from createOrder", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "quote used" }), { status: 409 }));
    await expect(
      createOrder("rider-1", { lat: 10.77, lng: 106.7 }, { lat: 10.78, lng: 106.71 }, "CAR", "q-1", 0),
    ).rejects.toBeInstanceOf(ApiError);
    expect(fetchMock.mock.calls.filter(([url]) => url === "/order-api/orders")).toHaveLength(1);
  });

  it("sends the promoToken with a booking, and only when there is one", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ orderId: "o-1", price: 87000, discount: 8700, finalPrice: 78300, promoCode: "WELCOME10" }), { status: 201 }),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify({ orderId: "o-2" }), { status: 201 }));
    const pickup = { lat: 10.77, lng: 106.7 };
    const dropoff = { lat: 10.78, lng: 106.71 };

    const booked = await createOrder("rider-1", pickup, dropoff, "CAR", "q-1", 0, "tok.sig");
    expect(booked).toMatchObject({ discount: 8700, finalPrice: 78300, promoCode: "WELCOME10" });
    await createOrder("rider-1", pickup, dropoff, "CAR", undefined, 0);

    const bodies = fetchMock.mock.calls.filter(([url]) => url === "/order-api/orders").map(([, init]) => JSON.parse(init.body));
    expect(bodies[0]).toMatchObject({ quoteId: "q-1", promoToken: "tok.sig" });
    expect(bodies[1]).not.toHaveProperty("promoToken");
  });

  it("explains a refused promo token on booking", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "the promo code has expired", code: "promo_expired" }), { status: 400 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "used", code: "promo_used" }), { status: 409 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "the price has changed", newQuote: {} }), { status: 409 }));
    const book = () => createOrder("rider-1", { lat: 10.77, lng: 106.7 }, { lat: 10.78, lng: 106.71 }, "CAR", undefined, 0, "t");

    expect(promoBookingErrorOf(await book().catch((e) => e))).toBe("promo_expired");
    expect(promoBookingErrorOf(await book().catch((e) => e))).toBe("promo_used");
    expect(promoBookingErrorOf(await book().catch((e) => e))).toBeNull(); // a re-quote is not a promo problem
    expect(promoBookingErrorOf(new Error("other"))).toBeNull();
  });

  it("validates a promo code and names the reason it is refused", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    const ok = { code: "WELCOME10", discount: 8700, price: 87000, finalPrice: 78300, currency: "VND", promoToken: "tok.sig", expiresAt: future };
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(ok), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "needs 40000", code: "min_fare" }), { status: 422 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "no such code", code: "not_found" }), { status: 404 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "price must be above 0 VND", code: "invalid_request" }), { status: 400 }));

    expect(await validatePromotion(" welcome10 ", 87000.4, "CAR")).toEqual(ok);
    const url = new URL(fetchMock.mock.calls[1][0], "http://x");
    expect(url.pathname).toBe("/promotion-api/promotions/validate");
    expect(Object.fromEntries(url.searchParams)).toEqual({ code: "welcome10", price: "87000", vehicleType: "CAR" });

    expect(promoRejectionOf(await validatePromotion("X", 1000, "CAR").catch((e) => e))).toBe("min_fare");
    expect(promoRejectionOf(await validatePromotion("X", 1000, "CAR").catch((e) => e))).toBe("not_found");
    expect(promoRejectionOf(await validatePromotion("X", 0, "CAR").catch((e) => e))).toBeNull(); // a bad request, not a refusal
    expect(promoRejectionOf(new Error("other"))).toBeNull();
  });

  it("lists, creates and disables promotions as an admin", async () => {
    signIn("admin", "ops-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    const promotion = {
      code: "SUMMER20", kind: "PERCENT", value: 20, maxDiscount: 30000, minFare: 0, vehicleType: null,
      validFrom: future, validTo: future, usageLimit: null, perRiderLimit: 1, disabledAt: null, status: "active",
      redeemed: 0, overLimit: 0, createdBy: "ops-1", createdAt: future,
    };
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-a", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ promotions: [promotion] }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(promotion), { status: 201 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...promotion, status: "disabled" }), { status: 200 }));

    expect(await listPromotions()).toEqual([promotion]);
    const created = await createPromotion({ code: "summer20", kind: "PERCENT", value: 20, maxDiscount: 30000, validTo: future });
    expect(created.code).toBe("SUMMER20");
    expect((await disablePromotion("SUMMER20")).status).toBe("disabled");

    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ role: "admin", id: "ops-1" });
    const [listCall, createCall, disableCall] = fetchMock.mock.calls.slice(1);
    expect(listCall[0]).toBe("/promotion-api/promotions");
    expect(createCall[0]).toBe("/promotion-api/promotions");
    expect(createCall[1].method).toBe("POST");
    expect(JSON.parse(createCall[1].body)).toEqual({ code: "summer20", kind: "PERCENT", value: 20, maxDiscount: 30000, validTo: future });
    expect(disableCall[0]).toBe("/promotion-api/promotions/SUMMER20/disable");
    expect(disableCall[1].method).toBe("POST");
    for (const [, init] of fetchMock.mock.calls.slice(1)) expect(new Headers(init.headers).get("Authorization")).toBe("Bearer jwt-a");
    forgetToken();
  });

  it("retries 404 in waitForTripByOrder until the trip exists", async () => {
    signIn("rider", "rider-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "trip not found" }), { status: 404 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "trip not found" }), { status: 404 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "t-1", orderId: "o-1", driverId: "driver-1", status: "ASSIGNED" }), {
          status: 200,
        }),
      );

    const trip = await waitForTripByOrder("o-1", 4, 5);
    expect(trip.id).toBe("t-1");
  });

  it("retries 404 in waitForOrder until the match is recorded, but not other errors", async () => {
    signIn("driver", "driver-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-d1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "order not found" }), { status: 404 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: "o-1", status: "MATCHED" }), { status: 200 }));
    expect((await waitForOrder("o-1", 4, 5)).id).toBe("o-1");

    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ error: "boom" }), { status: 500 }));
    await expect(waitForOrder("o-1", 4, 5)).rejects.toMatchObject({ status: 500 });
  });

  it("returns null on 404 for getActiveTrip and getLatestOrder, and calls other endpoints", async () => {
    signIn("driver", "driver-1");
    const future = new Date(Date.now() + 3600_000).toISOString();
    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify({ token: "jwt-d1", expiresAt: future }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "not found" }), { status: 404 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "not found" }), { status: 404 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ quoteId: "q-1", price: 30000 }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ distanceKm: 3.2, durationSec: 410, polyline: [], vehicle: "CAR", model: "detour-1.3" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ tripId: "t-1", status: "CANCELLED" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: "o-1", status: "CANCELLED" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ tripId: "t-1", stars: 5, tipAmount: 10000 }), { status: 201 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ driverId: "driver-1", average: 4.9, count: 10 }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ reviews: [{ tripId: "t-1", stars: 5 }] }), { status: 200 }));

    expect(await getActiveTrip("driver-1")).toBeNull();
    expect(await getLatestOrder("rider-1")).toBeNull();
    expect((await getQuote({ lat: 10, lng: 106 }, { lat: 10.1, lng: 106.1 }, "BIKE")).quoteId).toBe("q-1");
    expect((await getRoute({ lat: 10, lng: 106 }, { lat: 10.1, lng: 106.1 }, "CAR")).distanceKm).toBe(3.2);
    expect((await tripAction("t-1", "cancel", "Emergency")).status).toBe("CANCELLED");
    expect((await cancelOrder("o-1", "Changed plans")).status).toBe("CANCELLED");
    expect((await rateTrip("t-1", "rider-1", 5, "Great", 10000)).stars).toBe(5);
    expect((await getDriverRating("driver-1")).average).toBe(4.9);
    expect(await getDriverReviews("driver-1", { limit: 5 })).toHaveLength(1);
    forgetToken();
  });
});

describe("useNotificationSocket", () => {
  it("opens WebSocket with bearer subprotocol and dispatches parsed messages", async () => {
    const future = new Date(Date.now() + 3600_000).toISOString();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify({ token: "ws-jwt", expiresAt: future }), { status: 200 })),
    );

    const sockets: Array<{
      url: string;
      protocols?: string | string[];
      onopen?: () => void;
      onmessage?: (e: { data: string }) => void;
      onclose?: () => void;
      close: () => void;
    }> = [];

    class FakeWebSocket {
      url: string;
      protocols?: string | string[];
      onopen?: () => void;
      onmessage?: (e: { data: string }) => void;
      onclose?: () => void;
      close = vi.fn();
      constructor(url: string, protocols?: string | string[]) {
        this.url = url;
        this.protocols = protocols;
        sockets.push(this);
      }
    }
    vi.stubGlobal("WebSocket", FakeWebSocket);

    try {
      signIn("rider", "rider-1");
      const onMessage = vi.fn();
      const { result, unmount } = renderHook(() => useNotificationSocket("rider", "rider-1", onMessage));

      expect(result.current).toBe("connecting");
      await waitFor(() => expect(sockets).toHaveLength(1));
      expect(sockets[0].url).toContain("/ws/rider/rider-1");
      expect(sockets[0].protocols).toEqual(["bearer", "ws-jwt"]);

      act(() => {
        sockets[0].onopen?.();
      });
      expect(result.current).toBe("open");

      act(() => {
        sockets[0].onmessage?.({
          data: JSON.stringify({
            type: "order.matched",
            payload: { orderId: "o-1", driverId: "driver-1", vehicleType: "CAR" },
          }),
        });
      });
      expect(onMessage).toHaveBeenCalledWith({
        type: "order.matched",
        payload: { orderId: "o-1", driverId: "driver-1", vehicleType: "CAR" },
      });

      unmount();
      expect(sockets[0].close).toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
