import Redis from "ioredis";
import { env } from "./env.js";

export const redis = new Redis(env.redisUrl);

export const DRIVER_GEO_KEY = "drivers:geo";

export type DriverStatus = "AVAILABLE" | "ASSIGNED" | "ON_TRIP";

export function driverStatusKey(driverId: string): string {
  return `driver:${driverId}:status`;
}

export async function setDriverStatus(driverId: string, status: DriverStatus): Promise<void> {
  await redis.set(driverStatusKey(driverId), status);
}

export async function getDriverStatus(driverId: string): Promise<DriverStatus | null> {
  return (await redis.get(driverStatusKey(driverId))) as DriverStatus | null;
}

interface Candidate {
  driverId: string;
  distanceKm: number;
}

/** Nearest drivers to a point, closest first, regardless of status. */
export async function nearestDrivers(lat: number, lng: number, radiusKm: number): Promise<Candidate[]> {
  const results = (await redis.georadius(
    DRIVER_GEO_KEY,
    lng,
    lat,
    radiusKm,
    "km",
    "ASC",
    "WITHDIST",
    "COUNT",
    20,
  )) as [string, string][];

  return results.map(([driverId, distance]) => ({ driverId, distanceKm: Number(distance) }));
}

/** US-2.1: nearest AVAILABLE driver within radius, or null if none. */
export async function findNearestAvailableDriver(lat: number, lng: number, radiusKm: number): Promise<string | null> {
  const candidates = await nearestDrivers(lat, lng, radiusKm);
  for (const candidate of candidates) {
    const status = await getDriverStatus(candidate.driverId);
    if (status === "AVAILABLE") return candidate.driverId;
  }
  return null;
}
