import { pull, type Row } from "./ksql";

const num = (v: unknown): number => (typeof v === "number" ? v : Number(v ?? 0));

export interface Snapshot {
  matched: number;
  noDriver: number;
  avgMatchMs: number | null;
  trips: { completed: number; cancelled: number; inProgress: number };
  locationUpdatesPerMinute: number | null;
  ordersPerMinute: Array<{ minute: number; car: number; bike: number }>;
  matchByVehicle: Array<{ vehicle: string; matches: number; avgMs: number; maxMs: number }>;
  ratings: Array<{ driverId: string; average: number; count: number }>;
  /** Last few minutes of surge, newest first; empty if the surge_demand table doesn't exist yet. */
  surge: Array<{ minute: number; vehicle: string; orders: number; avgMultiplier: number; maxMultiplier: number }>;
}

export async function loadSnapshot(): Promise<Snapshot> {
  const [matched, noDriver, matchStats, tripStatus, locations, perMinute, ratings, surge] = await Promise.all([
    pull("SELECT ORDERS FROM matched_total;"),
    pull("SELECT ORDERS FROM no_driver_total;"),
    pull("SELECT VEHICLE_TYPE, MATCHES, AVG_MATCH_MS, MAX_MATCH_MS FROM match_stats;"),
    pull("SELECT STATUS, CHANGES FROM trip_status_totals;"),
    pull("SELECT WINDOWSTART, UPDATES FROM location_updates_per_minute;"),
    pull("SELECT VEHICLE_TYPE, WINDOWSTART, ORDERS FROM orders_per_minute;"),
    pull("SELECT DRIVER_ID, RATINGS, AVG_STARS FROM driver_rating_stats;"),
    // Optional: a ksqlDB set up before surge pricing has no such table.
    pull("SELECT VEHICLE_TYPE, WINDOWSTART, ORDERS, AVG_MULTIPLIER, MAX_MULTIPLIER FROM surge_demand;").catch((): Row[] => []),
  ]);

  const tripCount = (status: string) => num(tripStatus.find((r) => r.STATUS === status)?.CHANGES);

  // Weighted by matches, so a vehicle type with more matches counts for more.
  const totalMatches = matchStats.reduce((sum, r) => sum + num(r.MATCHES), 0);
  const avgMatchMs =
    totalMatches === 0 ? null : matchStats.reduce((sum, r) => sum + num(r.MATCHES) * num(r.AVG_MATCH_MS), 0) / totalMatches;

  // The newest minute is still filling up, so the last *complete* one is the fair throughput figure.
  const byMinute = [...locations].sort((a, b) => num(a.WINDOWSTART) - num(b.WINDOWSTART));
  const lastComplete = byMinute.length >= 2 ? byMinute[byMinute.length - 2] : undefined;

  const minutes = new Map<number, { minute: number; car: number; bike: number }>();
  for (const r of perMinute as Row[]) {
    const minute = num(r.WINDOWSTART);
    const entry = minutes.get(minute) ?? { minute, car: 0, bike: 0 };
    if (r.VEHICLE_TYPE === "BIKE") entry.bike += num(r.ORDERS);
    else entry.car += num(r.ORDERS);
    minutes.set(minute, entry);
  }

  return {
    matched: num(matched[0]?.ORDERS),
    noDriver: num(noDriver[0]?.ORDERS),
    avgMatchMs,
    trips: { completed: tripCount("COMPLETED"), cancelled: tripCount("CANCELLED"), inProgress: tripCount("IN_PROGRESS") },
    locationUpdatesPerMinute: lastComplete ? num(lastComplete.UPDATES) : null,
    ordersPerMinute: [...minutes.values()].sort((a, b) => a.minute - b.minute).slice(-10),
    matchByVehicle: matchStats.map((r) => ({
      vehicle: String(r.VEHICLE_TYPE),
      matches: num(r.MATCHES),
      avgMs: num(r.AVG_MATCH_MS),
      maxMs: num(r.MAX_MATCH_MS),
    })),
    ratings: ratings
      .map((r) => ({ driverId: String(r.DRIVER_ID), average: num(r.AVG_STARS), count: num(r.RATINGS) }))
      .sort((a, b) => b.average - a.average || b.count - a.count),
    surge: surge
      .map((r) => ({
        minute: num(r.WINDOWSTART),
        vehicle: String(r.VEHICLE_TYPE),
        orders: num(r.ORDERS),
        avgMultiplier: num(r.AVG_MULTIPLIER),
        maxMultiplier: num(r.MAX_MULTIPLIER),
      }))
      .sort((a, b) => b.minute - a.minute)
      .slice(0, 6),
  };
}
