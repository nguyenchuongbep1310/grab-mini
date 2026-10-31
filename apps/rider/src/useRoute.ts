import { useEffect, useRef, useState } from "react";
import { getRoute, type LatLng, type Route, type VehicleType } from "@grab-mini/client";

// Wait for the rider to stop moving the pin or switching vehicles before asking.
const DEBOUNCE_MS = 300;

/** The driver's ETA is recomputed at most this often, however fast locations are pushed. */
export const ETA_INTERVAL_MS = 10_000;

/**
 * Distance and travel time of the trip being planned (pickup -> dropoff). Debounced
 * like useQuote. It is only decoration: if Routing Service is down the route stays
 * null and booking is unaffected.
 */
export function useRoute(pickup: LatLng, dropoff: LatLng | null, vehicle: VehicleType, enabled: boolean): Route | null {
  const [route, setRoute] = useState<Route | null>(null);
  const active = enabled && dropoff !== null;
  const key = active ? `${pickup.lat},${pickup.lng}|${dropoff.lat},${dropoff.lng}|${vehicle}` : "";

  useEffect(() => {
    // A different trip: the old route says nothing about it.
    setRoute(null);
    if (!active) return;
    let stale = false;
    const timer = setTimeout(() => {
      getRoute(pickup, dropoff, vehicle)
        .then((r) => !stale && setRoute(r))
        .catch(() => !stale && setRoute(null));
    }, DEBOUNCE_MS);
    return () => {
      stale = true;
      clearTimeout(timer);
    };
    // pickup and dropoff are covered by key.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  return route;
}

/**
 * Time left from `from` (the driver's pushed position) to `to` (the pickup, then the
 * dropoff). Locations arrive every few seconds, so lookups are throttled: the first
 * one goes out at once, later ones at most once per `intervalMs`, using the newest
 * position when the wait is over. A failed lookup clears the estimate.
 */
export function useEta(
  from: LatLng | null,
  to: LatLng | null,
  vehicle: VehicleType,
  enabled: boolean,
  intervalMs = ETA_INTERVAL_MS,
): Route | null {
  const [route, setRoute] = useState<Route | null>(null);
  const latest = useRef({ from, to, vehicle });
  latest.current = { from, to, vehicle };
  const lastAt = useRef<number | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const generation = useRef(0);

  const active = enabled && from !== null && to !== null;
  const toKey = enabled && to ? `${to.lat},${to.lng}|${vehicle}` : "";
  const fromKey = from ? `${from.lat},${from.lng}` : "";

  // A new target (pickup -> dropoff) or no longer needed: drop the estimate, the lookup
  // in flight and the one waiting. The first lookup for a new target goes out at once, so
  // the throttle clock restarts (it also keeps a StrictMode remount from delaying it).
  useEffect(() => {
    setRoute(null);
    return () => {
      generation.current++;
      if (timer.current) clearTimeout(timer.current);
      timer.current = null;
      lastAt.current = null;
    };
  }, [toKey]);

  useEffect(() => {
    if (!active || timer.current) return; // a waiting lookup will use the newest position
    const lookup = () => {
      const { from, to, vehicle } = latest.current;
      if (!from || !to) return;
      lastAt.current = Date.now();
      const mine = generation.current;
      getRoute(from, to, vehicle)
        .then((r) => mine === generation.current && setRoute(r))
        .catch(() => mine === generation.current && setRoute(null));
    };
    const wait = lastAt.current === null ? 0 : Math.max(0, intervalMs - (Date.now() - lastAt.current));
    if (wait === 0) {
      lookup();
    } else {
      timer.current = setTimeout(() => {
        timer.current = null;
        lookup();
      }, wait);
    }
  }, [active, fromKey, toKey, intervalMs]);

  return route;
}
