import { describe, expect, it, vi } from "vitest";
import * as ksql from "./ksql";
import { loadSnapshot } from "./stats";

describe("ops stats loadSnapshot", () => {
  it("computes weighted match time, last complete minute throughput, and sorts ratings & surge", async () => {
    vi.spyOn(ksql, "pull").mockImplementation(async (sql: string) => {
      if (sql.includes("matched_total")) return [{ ORDERS: 18 }];
      if (sql.includes("no_driver_total")) return [{ ORDERS: 2 }];
      if (sql.includes("match_stats")) {
        return [
          { VEHICLE_TYPE: "CAR", MATCHES: 10, AVG_MATCH_MS: 1000, MAX_MATCH_MS: 2000 },
          { VEHICLE_TYPE: "BIKE", MATCHES: 8, AVG_MATCH_MS: 2000, MAX_MATCH_MS: 3500 },
        ];
      }
      if (sql.includes("trip_status_totals")) {
        return [
          { STATUS: "COMPLETED", CHANGES: 15 },
          { STATUS: "CANCELLED", CHANGES: 3 },
          { STATUS: "IN_PROGRESS", CHANGES: 2 },
        ];
      }
      if (sql.includes("location_updates_per_minute")) {
        return [
          { WINDOWSTART: 1000, UPDATES: 120 },
          { WINDOWSTART: 2000, UPDATES: 45 }, // newest (incomplete) minute
        ];
      }
      if (sql.includes("orders_per_minute")) {
        return [
          { VEHICLE_TYPE: "CAR", WINDOWSTART: 1000, ORDERS: 5 },
          { VEHICLE_TYPE: "BIKE", WINDOWSTART: 1000, ORDERS: 3 },
        ];
      }
      if (sql.includes("driver_rating_stats")) {
        return [
          { DRIVER_ID: "driver-2", RATINGS: 4, AVG_STARS: 4.5 },
          { DRIVER_ID: "driver-1", RATINGS: 6, AVG_STARS: 4.9 },
        ];
      }
      if (sql.includes("surge_demand")) {
        return [{ VEHICLE_TYPE: "CAR", WINDOWSTART: 2000, ORDERS: 7, AVG_MULTIPLIER: 1.3, MAX_MULTIPLIER: 1.6 }];
      }
      return [];
    });

    const snap = await loadSnapshot();
    expect(snap.matched).toBe(18);
    expect(snap.noDriver).toBe(2);
    expect(snap.avgMatchMs).toBeCloseTo((10 * 1000 + 8 * 2000) / 18, 2);
    expect(snap.trips).toEqual({ completed: 15, cancelled: 3, inProgress: 2 });
    expect(snap.locationUpdatesPerMinute).toBe(120);
    expect(snap.ordersPerMinute).toEqual([{ minute: 1000, car: 5, bike: 3 }]);
    expect(snap.ratings.map((r) => r.driverId)).toEqual(["driver-1", "driver-2"]);
    expect(snap.surge).toHaveLength(1);
    expect(snap.surge[0].maxMultiplier).toBe(1.6);
  });
});
