import { useCallback, useEffect, useRef, useState } from "react";
import { getQuote, type LatLng, type Quote, type VehicleType } from "@grab-mini/client";

export type QuoteStatus = "idle" | "loading" | "ready" | "error";

export interface QuoteState {
  /** The price to show and book with; null until the first one arrives for this trip. */
  quote: Quote | null;
  status: QuoteStatus;
  /** True after a booking was refused because the price moved: the rider must confirm the new one. */
  changed: boolean;
  /** Takes the fresh quote from a 409 and asks the rider to confirm it. */
  applyNewQuote: (quote: Quote) => void;
  /** The rider confirmed the changed price. */
  acknowledge: () => void;
}

// Wait for the rider to stop moving the pin or switching vehicles before asking.
const DEBOUNCE_MS = 300;
// Ask again a little before a quote expires, so the one on screen is always bookable.
const REFRESH_BEFORE_EXPIRY_MS = 5000;
const MIN_REFRESH_MS = 1000;

/**
 * The price of the trip being planned. Refetches (debounced) when the pickup,
 * dropoff or vehicle changes, and again shortly before the quote expires. If
 * pricing is unavailable the status is "error" and booking still works: Order
 * Service then prices the ride itself.
 */
export function useQuote(pickup: LatLng, dropoff: LatLng | null, vehicleType: VehicleType, enabled: boolean): QuoteState {
  const [quote, setQuote] = useState<Quote | null>(null);
  const [status, setStatus] = useState<QuoteStatus>("idle");
  const [changed, setChanged] = useState(false);
  const [nonce, setNonce] = useState(0);
  const lastKey = useRef("");

  const active = enabled && dropoff !== null;
  const key = active ? `${pickup.lat},${pickup.lng}|${dropoff.lat},${dropoff.lng}|${vehicleType}` : "";

  useEffect(() => {
    if (lastKey.current !== key) {
      // A different trip: the old price says nothing about it.
      lastKey.current = key;
      setQuote(null);
      setChanged(false);
    }
    if (!active) {
      setStatus("idle");
      return;
    }
    let stale = false;
    setStatus((s) => (s === "ready" ? s : "loading"));
    const timer = setTimeout(() => {
      getQuote(pickup, dropoff, vehicleType)
        .then((q) => {
          if (stale) return;
          setQuote(q);
          setStatus("ready");
        })
        .catch(() => {
          if (stale) return;
          setStatus("error");
        });
    }, DEBOUNCE_MS);
    return () => {
      stale = true;
      clearTimeout(timer);
    };
    // pickup and dropoff are covered by key.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, nonce]);

  // Ask for a new quote before this one expires.
  const expiresAt = quote?.expiresAt;
  useEffect(() => {
    if (!expiresAt || !active) return;
    const wait = Math.max(Date.parse(expiresAt) - Date.now() - REFRESH_BEFORE_EXPIRY_MS, MIN_REFRESH_MS);
    const timer = setTimeout(() => setNonce((n) => n + 1), wait);
    return () => clearTimeout(timer);
  }, [expiresAt, active]);

  const applyNewQuote = useCallback((q: Quote) => {
    setQuote(q);
    setStatus("ready");
    setChanged(true);
  }, []);
  const acknowledge = useCallback(() => setChanged(false), []);

  return { quote, status, changed, applyNewQuote, acknowledge };
}
