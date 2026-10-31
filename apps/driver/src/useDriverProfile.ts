import { useCallback, useEffect, useRef, useState } from "react";
import { getDriverProfile, type DriverProfile } from "@grab-mini/client";
import { classifyFailure, gateOf, type Gate, type ProfileLoad } from "./onboarding";

/** How often a driver waiting for review is re-checked, so the app unlocks once an admin approves. */
export const PENDING_POLL_MS = 10_000;

/**
 * The driver's profile and the screen it implies. Loaded once on start. A later refresh that fails keeps what
 * we already knew (an outage must not flip an approved driver to "unknown" or a pending one to approved);
 * only the first load can end as unavailable or denied. While PENDING it re-checks every PENDING_POLL_MS.
 */
export function useDriverProfile(driverId: string) {
  const [load, setLoad] = useState<ProfileLoad>({ kind: "loading" });
  const known = useRef<DriverProfile | null>(null);
  const alive = useRef(true);

  const refresh = useCallback(async () => {
    try {
      const profile = await getDriverProfile(driverId);
      if (!alive.current) return;
      known.current = profile;
      setLoad({ kind: "loaded", profile });
    } catch (err) {
      if (!alive.current || known.current) return;
      setLoad({ kind: classifyFailure(err) });
    }
  }, [driverId]);

  useEffect(() => {
    alive.current = true;
    known.current = null;
    setLoad({ kind: "loading" });
    refresh();
    return () => {
      alive.current = false;
    };
  }, [refresh]);

  const waiting = load.kind === "loaded" && load.profile.status === "PENDING";
  useEffect(() => {
    if (!waiting) return;
    const timer = setInterval(refresh, PENDING_POLL_MS);
    return () => clearInterval(timer);
  }, [waiting, refresh]);

  /** A profile we just got back from a save or an apply. */
  const accept = useCallback((profile: DriverProfile) => {
    known.current = profile;
    setLoad({ kind: "loaded", profile });
  }, []);

  /** Try again after "unavailable" or "denied": goes back to checking. */
  const retry = useCallback(() => {
    known.current = null;
    setLoad({ kind: "loading" });
    return refresh();
  }, [refresh]);

  const gate: Gate = gateOf(load);
  return { gate, refresh, accept, retry };
}
