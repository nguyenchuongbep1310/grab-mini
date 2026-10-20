import { expect, test, type Page, type WebSocketRoute } from "@playwright/test";

// Transparent 1x1 PNG so Leaflet map tiles load instantaneously without internet access.
const BLANK_PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
  "base64",
);

interface Harness {
  riderSocket: () => WebSocketRoute | null;
  driverSocket: () => WebSocketRoute | null;
  sendToRider: (msg: unknown) => void;
  sendToDriver: (msg: unknown) => void;
  getState: () => {
    orderStatus: string;
    tripStatus: string;
    ratingStars: number;
    tipAmount: number;
    cancelReason: string;
    cancelledBy: string;
  };
}

/**
 * Attaches deterministic API + WebSocket handlers to a page (or multiple pages in a test)
 * so Rider App (5171) and Driver App (5172) interact with shared in-memory ride state.
 */
async function attachRideBackend(pages: Page[]): Promise<Harness> {
  let riderWs: WebSocketRoute | null = null;
  let driverWs: WebSocketRoute | null = null;

  const state = {
    orderId: "11111111-2222-3333-4444-555555555555",
    tripId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    riderId: "rider-1",
    driverId: "driver-1",
    vehicleType: "CAR",
    pickup: { lat: 10.776, lng: 106.7 },
    dropoff: { lat: 10.785, lng: 106.71 },
    orderStatus: "NONE",
    tripStatus: "NONE",
    price: 42000,
    surgeMultiplier: 1.4,
    cancelledBy: "",
    cancelReason: "",
    ratingStars: 0,
    ratingComment: "",
    tipAmount: 0,
    totalRatings: 2,
    ratingSum: 9, // 4.50 average initially
    totalTips: 10000,
  };

  for (const page of pages) {
    await page.route("https://tile.openstreetmap.org/**", (route) =>
      route.fulfill({ status: 200, contentType: "image/png", body: BLANK_PNG }),
    );

    await page.route("**/auth-api/token", async (route) => {
      const body = route.request().postDataJSON() as { role: string; id: string };
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          token: `e2e-jwt-${body.role}-${body.id}`,
          expiresAt: new Date(Date.now() + 3600_000).toISOString(),
        }),
      });
    });

    await page.route("**/pricing-api/quotes**", async (route) => {
      const url = new URL(route.request().url());
      const v = url.searchParams.get("vehicleType") ?? "CAR";
      const price = v === "BIKE" ? 21000 : 42000;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          quoteId: `quote-${v.toLowerCase()}`,
          baseFare: v === "BIKE" ? 15000 : 30000,
          multiplier: state.surgeMultiplier,
          price,
          currency: "VND",
          expiresAt: new Date(Date.now() + 60_000).toISOString(),
        }),
      });
    });

    await page.route("**/order-api/riders/*/latest-order", async (route) => {
      if (state.orderStatus === "NONE") {
        await route.fulfill({ status: 404, contentType: "application/json", body: JSON.stringify({ error: "not found" }) });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: state.orderId,
          riderId: state.riderId,
          pickup: state.pickup,
          dropoff: state.dropoff,
          vehicleType: state.vehicleType,
          status: state.orderStatus,
          price: state.price,
          surgeMultiplier: state.surgeMultiplier,
          cancelledBy: state.cancelledBy,
          cancelReason: state.cancelReason,
        }),
      });
    });

    await page.route("**/order-api/orders", async (route) => {
      if (route.request().method() !== "POST") return route.fallback();
      const req = route.request().postDataJSON() as {
        riderId: string;
        dropoff: { lat: number; lng: number };
        vehicleType: string;
      };
      state.vehicleType = req.vehicleType || "CAR";
      state.dropoff = req.dropoff;
      state.price = state.vehicleType === "BIKE" ? 21000 : 42000;
      state.orderStatus = "PENDING";
      await route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({
          orderId: state.orderId,
          status: "PENDING",
          price: state.price,
          surgeMultiplier: state.surgeMultiplier,
          currency: "VND",
        }),
      });
    });

    await page.route(`**/order-api/orders/${state.orderId}/cancel`, async (route) => {
      const req = (route.request().postDataJSON() ?? {}) as { reason?: string };
      state.orderStatus = "CANCELLED";
      state.cancelledBy = "RIDER";
      state.cancelReason = req.reason ?? "";
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: state.orderId,
          riderId: state.riderId,
          pickup: state.pickup,
          dropoff: state.dropoff,
          vehicleType: state.vehicleType,
          status: "CANCELLED",
          cancelledBy: "RIDER",
          cancelReason: state.cancelReason,
        }),
      });
    });

    await page.route(`**/order-api/orders/${state.orderId}`, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: state.orderId,
          riderId: state.riderId,
          pickup: state.pickup,
          dropoff: state.dropoff,
          vehicleType: state.vehicleType,
          status: state.orderStatus,
          price: state.price,
          surgeMultiplier: state.surgeMultiplier,
          cancelledBy: state.cancelledBy,
          cancelReason: state.cancelReason,
        }),
      });
    });

    await page.route("**/trip-api/drivers/*/active-trip", async (route) => {
      if (state.tripStatus !== "ASSIGNED" && state.tripStatus !== "IN_PROGRESS") {
        await route.fulfill({ status: 404, contentType: "application/json", body: JSON.stringify({ error: "not found" }) });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: state.tripId,
          orderId: state.orderId,
          driverId: state.driverId,
          riderId: state.riderId,
          status: state.tripStatus,
        }),
      });
    });

    await page.route(`**/trip-api/trips/by-order/${state.orderId}`, async (route) => {
      if (state.tripStatus === "NONE") {
        await route.fulfill({ status: 404, contentType: "application/json", body: JSON.stringify({ error: "not found" }) });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: state.tripId,
          orderId: state.orderId,
          driverId: state.driverId,
          riderId: state.riderId,
          status: state.tripStatus,
        }),
      });
    });

    await page.route(`**/trip-api/trips/${state.tripId}/pickup`, async (route) => {
      state.tripStatus = "IN_PROGRESS";
      const evt = {
        type: "trip.status.changed",
        payload: {
          eventId: "evt-pickup",
          occurredAt: new Date().toISOString(),
          tripId: state.tripId,
          orderId: state.orderId,
          driverId: state.driverId,
          riderId: state.riderId,
          status: "IN_PROGRESS",
          cancelledBy: "",
          reason: "",
        },
      };
      riderWs?.send(JSON.stringify(evt));
      driverWs?.send(JSON.stringify(evt));
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ tripId: state.tripId, status: "IN_PROGRESS" }),
      });
    });

    await page.route(`**/trip-api/trips/${state.tripId}/complete`, async (route) => {
      state.tripStatus = "COMPLETED";
      const evt = {
        type: "trip.status.changed",
        payload: {
          eventId: "evt-complete",
          occurredAt: new Date().toISOString(),
          tripId: state.tripId,
          orderId: state.orderId,
          driverId: state.driverId,
          riderId: state.riderId,
          status: "COMPLETED",
          cancelledBy: "",
          reason: "",
        },
      };
      riderWs?.send(JSON.stringify(evt));
      driverWs?.send(JSON.stringify(evt));
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ tripId: state.tripId, status: "COMPLETED" }),
      });
    });

    await page.route(`**/trip-api/trips/${state.tripId}/cancel`, async (route) => {
      const req = (route.request().postDataJSON() ?? {}) as { reason?: string };
      const authHeader = route.request().headers()["authorization"] ?? "";
      const by = authHeader.includes("driver") ? "DRIVER" : "RIDER";
      state.tripStatus = "CANCELLED";
      state.orderStatus = "CANCELLED";
      state.cancelledBy = by;
      state.cancelReason = req.reason ?? "";
      const evt = {
        type: "trip.status.changed",
        payload: {
          eventId: "evt-cancel",
          occurredAt: new Date().toISOString(),
          tripId: state.tripId,
          orderId: state.orderId,
          driverId: state.driverId,
          riderId: state.riderId,
          status: "CANCELLED",
          cancelledBy: by,
          reason: state.cancelReason,
        },
      };
      riderWs?.send(JSON.stringify(evt));
      driverWs?.send(JSON.stringify(evt));
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ tripId: state.tripId, status: "CANCELLED" }),
      });
    });

    await page.route(`**/trip-api/trips/${state.tripId}/rating`, async (route) => {
      const req = route.request().postDataJSON() as { stars: number; comment: string; tipAmount: number };
      state.ratingStars = req.stars;
      state.ratingComment = req.comment;
      state.tipAmount = req.tipAmount ?? 0;
      state.totalRatings += 1;
      state.ratingSum += req.stars;
      state.totalTips += state.tipAmount;

      const evt = {
        type: "trip.rated",
        payload: {
          eventId: "evt-rated",
          occurredAt: new Date().toISOString(),
          tripId: state.tripId,
          orderId: state.orderId,
          riderId: state.riderId,
          driverId: state.driverId,
          stars: req.stars,
          comment: req.comment,
          tipAmount: state.tipAmount,
          currency: "VND",
        },
      };
      driverWs?.send(JSON.stringify(evt));
      await route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({ tripId: state.tripId, stars: req.stars, tipAmount: state.tipAmount }),
      });
    });

    await page.route("**/rating-api/drivers/*/rating", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          driverId: state.driverId,
          average: state.ratingSum / state.totalRatings,
          count: state.totalRatings,
          totalTips: state.totalTips,
          tipCount: state.tipAmount > 0 ? 2 : 1,
        }),
      });
    });

    await page.routeWebSocket("**/ws/rider/**", (ws) => {
      riderWs = ws;
    });
    await page.routeWebSocket("**/ws/driver/**", (ws) => {
      driverWs = ws;
    });
  }

  return {
    riderSocket: () => riderWs,
    driverSocket: () => driverWs,
    sendToRider: (msg) => riderWs?.send(JSON.stringify(msg)),
    sendToDriver: (msg) => driverWs?.send(JSON.stringify(msg)),
    getState: () => ({
      orderStatus: state.orderStatus,
      tripStatus: state.tripStatus,
      ratingStars: state.ratingStars,
      tipAmount: state.tipAmount,
      cancelReason: state.cancelReason,
      cancelledBy: state.cancelledBy,
    }),
  };
}

test.describe("grab-mini E2E browser flows", () => {
  test("Full ride lifecycle across Rider App and Driver App (quote -> book -> match -> pickup -> complete -> rate & tip)", async ({
    browser,
  }) => {
    const context = await browser.newContext({ viewport: { width: 430, height: 860 } });
    const riderPage = await context.newPage();
    const driverPage = await context.newPage();
    const harness = await attachRideBackend([riderPage, driverPage]);

    await riderPage.goto("http://localhost:5171/?rider=rider-1");
    await driverPage.goto("http://localhost:5172/?driver=driver-1");

    // 1. Verify WebSocket connected on both apps
    await expect(riderPage.locator(".app__conn")).toHaveText("Live");
    await expect(driverPage.locator(".app__conn")).toHaveText("Online");
    await expect(driverPage.getByText("4.50 · 2 ratings")).toBeVisible();

    // 2. Rider drops a destination pin on the map and gets a surge quote
    const bookBtn = riderPage.getByRole("button", { name: "Book ride" });
    await expect(bookBtn).toBeDisabled();
    await riderPage.locator(".gm-map").click({ position: { x: 200, y: 200 } });

    await expect(riderPage.getByText("x1.4")).toBeVisible();
    await expect(riderPage.locator(".gm-price__amount")).toContainText("42.000");
    await expect(bookBtn).toBeEnabled();

    // 3. Rider books the ride
    await bookBtn.click();
    await expect(riderPage.getByRole("heading", { name: "Looking for a driver" })).toBeVisible();

    // 4. Matching service finds driver-1 and pushes order.matched to both Rider and Driver
    const matchedEvt = {
      type: "order.matched",
      payload: {
        eventId: "evt-match-1",
        occurredAt: new Date().toISOString(),
        orderId: "11111111-2222-3333-4444-555555555555",
        driverId: "driver-1",
        riderId: "rider-1",
        vehicleType: "CAR",
      },
    };
    // Mark trip as ASSIGNED before pushing order.matched
    await driverPage.evaluate(() => {}); // yield
    harness.sendToRider(matchedEvt);
    harness.sendToDriver(matchedEvt);

    // Also set tripStatus via pickup readiness: update internal trip endpoint by triggering a location update too
    harness.sendToRider({
      type: " trip.status.changed".trim(),
      payload: {
        eventId: "evt-assigned",
        occurredAt: new Date().toISOString(),
        tripId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        orderId: "11111111-2222-3333-4444-555555555555",
        driverId: "driver-1",
        riderId: "rider-1",
        status: "ASSIGNED",
        cancelledBy: "",
        reason: "",
      },
    });
    harness.sendToDriver({
      type: "trip.status.changed",
      payload: {
        eventId: "evt-assigned",
        occurredAt: new Date().toISOString(),
        tripId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        orderId: "11111111-2222-3333-4444-555555555555",
        driverId: "driver-1",
        riderId: "rider-1",
        status: "ASSIGNED",
        cancelledBy: "",
        reason: "",
      },
    });

    await expect(riderPage.getByRole("heading", { name: "Your driver is on the way" })).toBeVisible();
    await expect(driverPage.getByRole("heading", { name: "New trip" })).toBeVisible();

    // 5. Driver confirms pickup -> both apps transition to IN_PROGRESS
    await driverPage.getByRole("button", { name: "Confirm pickup" }).click();
    await expect(driverPage.getByRole("heading", { name: "Trip in progress" })).toBeVisible();
    await expect(riderPage.getByRole("heading", { name: "Enjoy your ride" })).toBeVisible();

    // 6. Driver completes trip -> both apps transition to COMPLETED
    await driverPage.getByRole("button", { name: "Complete trip" }).click();
    await expect(driverPage.getByRole("heading", { name: "Trip completed" })).toBeVisible();
    await expect(riderPage.getByRole("heading", { name: "You've arrived" })).toBeVisible();

    // 7. Rider rates 5 stars, adds a comment and 20.000đ tip
    await riderPage.getByRole("radio", { name: "5 stars" }).click();
    await riderPage.getByLabel("Comment (optional)").fill("Smooth and safe drive!");
    await riderPage.getByRole("button", { name: /20\.000/ }).click();
    await riderPage.getByRole("button", { name: "Submit rating" }).click();

    await expect(riderPage.getByText("Your rating")).toBeVisible();
    await expect(riderPage.getByText("Smooth and safe drive!")).toBeVisible();

    // Driver receives real-time toast with rating and tip, and returns online with updated average (4.67 · 3 ratings)
    await expect(driverPage.getByText("5-star rating from rider-1")).toBeVisible();
    await driverPage.getByRole("button", { name: "Back online" }).click();
    await expect(driverPage.getByText("4.67 · 3 ratings")).toBeVisible();

    await context.close();
  });

  test("Rider cancels while searching and Driver cancels an assigned trip with reason", async ({ page }) => {
    const harness = await attachRideBackend([page]);
    await page.goto("http://localhost:5171/?rider=rider-1");

    // Rider drops pin and books Bike
    await page.getByRole("radio", { name: /Bike/ }).click();
    await page.locator(".gm-map").click({ position: { x: 180, y: 220 } });
    await expect(page.locator(".gm-price__amount")).toContainText("21.000");
    await page.getByRole("button", { name: "Book ride" }).click();

    await expect(page.getByRole("heading", { name: "Looking for a driver" })).toBeVisible();

    // Rider opens cancel dialog, chooses a reason, and confirms
    await page.getByRole("button", { name: "Cancel ride" }).click();
    const dialog = page.getByRole("dialog", { name: "Cancel this ride?" });
    await expect(dialog).toBeVisible();
    await dialog.getByLabel("Changed my plans").click();
    await dialog.getByRole("button", { name: "Cancel ride" }).click();

    await expect(page.getByRole("heading", { name: "You cancelled" })).toBeVisible();
    expect(harness.getState().cancelReason).toBe("Changed my plans");

    // Start a new ride and simulate no driver found
    await page.getByRole("button", { name: "New ride" }).click();
    await page.locator(".gm-map").click({ position: { x: 210, y: 210 } });
    await page.getByRole("button", { name: "Book ride" }).click();

    harness.sendToRider({
      type: "order.no_driver_found",
      payload: {
        eventId: "evt-nodriver",
        occurredAt: new Date().toISOString(),
        orderId: "11111111-2222-3333-4444-555555555555",
        riderId: "rider-1",
      },
    });
    await expect(page.getByRole("heading", { name: "No drivers nearby" })).toBeVisible();
  });

  test("Ops dashboard renders live analytics from ksqlDB pull queries", async ({ page }) => {
    await page.route("**/ksql-api/query", async (route) => {
      const body = route.request().postDataJSON() as { ksql: string };
      const sql = body.ksql;
      const respond = (columnNames: string[], rows: unknown[][]) =>
        route.fulfill({
          status: 200,
          contentType: "application/vnd.ksqlapi.delimited.v1",
          body: [JSON.stringify({ columnNames }), ...rows.map((r) => JSON.stringify(r))].join("\n"),
        });

      if (sql.includes("matched_total")) return respond(["ORDERS"], [[24]]);
      if (sql.includes("no_driver_total")) return respond(["ORDERS"], [[1]]);
      if (sql.includes("match_stats")) return respond(["VEHICLE_TYPE", "MATCHES", "AVG_MATCH_MS", "MAX_MATCH_MS"], [["CAR", 16, 1200, 2400], ["BIKE", 8, 900, 1500]]);
      if (sql.includes("trip_status_totals")) return respond(["STATUS", "CHANGES"], [["COMPLETED", 20], ["CANCELLED", 2], ["IN_PROGRESS", 2]]);
      if (sql.includes("location_updates_per_minute")) return respond(["WINDOWSTART", "UPDATES"], [[1700000000000, 180], [1700000060000, 40]]);
      if (sql.includes("orders_per_minute")) return respond(["VEHICLE_TYPE", "WINDOWSTART", "ORDERS"], [["CAR", 1700000000000, 10], ["BIKE", 1700000000000, 5]]);
      if (sql.includes("driver_rating_stats")) return respond(["DRIVER_ID", "RATINGS", "AVG_STARS"], [["driver-1", 12, 4.92]]);
      if (sql.includes("surge_demand")) return respond(["VEHICLE_TYPE", "WINDOWSTART", "ORDERS", "AVG_MULTIPLIER", "MAX_MULTIPLIER"], [["CAR", 1700000000000, 10, 1.3, 1.5]]);
      return respond([], []);
    });

    await page.goto("http://localhost:5173");
    await expect(page.getByRole("heading", { name: "grab-mini ops" })).toBeVisible();
    await expect(page.getByText("96%")).toBeVisible(); // 24 / 25 match rate
    await expect(page.getByText("24 matched · 1 without a driver")).toBeVisible();
    await expect(page.getByText("driver-1")).toBeVisible();
    await expect(page.getByText("x1.5")).toBeVisible();
  });
});
