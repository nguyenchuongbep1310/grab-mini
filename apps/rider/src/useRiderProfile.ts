import { useCallback, useEffect, useState } from "react";
import { getRiderProfile, getDriverPublic, type DriverPublic, type RiderProfile } from "@grab-mini/client";

export type ProfileLoad = "loading" | "ready" | "error";

/**
 * The rider's own profile, loaded once on start. It only decorates (the avatar's initials and the profile
 * screen): if it can't be loaded the app works as before and the avatar falls back to the id.
 */
export function useRiderProfile(riderId: string) {
  const [profile, setProfile] = useState<RiderProfile | null>(null);
  const [status, setStatus] = useState<ProfileLoad>("loading");

  const load = useCallback(() => {
    let stale = false;
    setStatus("loading");
    getRiderProfile(riderId)
      .then((p) => {
        if (stale) return;
        setProfile(p);
        setStatus("ready");
      })
      .catch(() => !stale && setStatus("error"));
    return () => {
      stale = true;
    };
  }, [riderId]);

  useEffect(() => load(), [load]);

  return { profile, status, reload: load, saved: setProfile };
}

/**
 * Who the matched driver is (name, vehicle, plate), asked once per driver id. Never blocks the ride: until it
 * answers, and when there is no approved profile (null) or the call fails (rate limit, service down), the
 * result stays null and the card shows the id as it always did.
 */
export function useDriverCard(driverId: string | null): DriverPublic | null {
  const [card, setCard] = useState<{ driverId: string; driver: DriverPublic } | null>(null);

  useEffect(() => {
    if (!driverId) return;
    let stale = false;
    getDriverPublic(driverId)
      .then((driver) => {
        if (!stale && driver) setCard({ driverId, driver });
      })
      .catch(() => {});
    return () => {
      stale = true;
    };
  }, [driverId]);

  // A card for a previous driver must never be shown for the current one.
  return card && card.driverId === driverId ? card.driver : null;
}
