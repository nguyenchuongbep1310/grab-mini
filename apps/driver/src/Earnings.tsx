import { useEffect, useState } from "react";
import { Card } from "@grab-mini/ui";
import { getDriverEarnings, getTripPayments, type DriverEarnings, type DriverPayout } from "@grab-mini/client";

const vnd = (n: number) => `${n.toLocaleString("vi-VN")}đ`;

/** What the driver earned from a trip's charges: the sum of their share (after commission), fare and tip apart. */
export function summarizePayouts(payouts: DriverPayout[]) {
  const share = (kind: DriverPayout["kind"]) => payouts.filter((p) => p.kind === kind).reduce((s, p) => s + p.amount, 0);
  const fare = share("FARE");
  const tip = share("TIP");
  const cancellationFee = share("CANCELLATION_FEE");
  return { fare, tip, cancellationFee, total: fare + tip + cancellationFee, commission: payouts.reduce((s, p) => s + p.commission, 0) };
}

/**
 * Polls briefly: Payment Service settles the fare a moment after the trip
 * completes and the tip a moment after the rider rates. Once a rating has
 * arrived (refreshKey above 0) it keeps polling its full budget to catch the tip.
 */
export function useTripPayouts(tripId: string | null, refreshKey: number, intervalMs = 1500, attempts = 8) {
  const [payouts, setPayouts] = useState<DriverPayout[] | null>(null);

  useEffect(() => {
    if (!tripId) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let left = attempts;
    const poll = async () => {
      try {
        const res = await getTripPayments(tripId);
        if (stopped) return;
        const got = res.payouts ?? [];
        setPayouts(got);
        if (got.some((p) => p.kind === "FARE") && refreshKey === 0) return;
      } catch {
        // Not settled yet, or briefly unreachable: try again.
      }
      if (stopped || --left <= 0) return;
      timer = setTimeout(poll, intervalMs);
    };
    poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [tripId, refreshKey, intervalMs, attempts]);

  return payouts;
}

export interface TripEarningsProps {
  tripId: string | null;
  /** Bump when a rating (and so maybe a tip) arrives, to look again. */
  refreshKey: number;
}

/** "You earned" for the trip just finished. Renders nothing until something has been settled. */
export function TripEarnings({ tripId, refreshKey }: TripEarningsProps) {
  const payouts = useTripPayouts(tripId, refreshKey);
  if (!payouts || payouts.length === 0) return null;
  const s = summarizePayouts(payouts);
  return (
    <Card padding="sm">
      <dl className="trip-facts" aria-label="Trip earnings">
        {s.fare > 0 && (
          <>
            <dt>Fare</dt>
            <dd>{vnd(s.fare)}</dd>
          </>
        )}
        {s.tip > 0 && (
          <>
            <dt>Tip</dt>
            <dd>{vnd(s.tip)}</dd>
          </>
        )}
        {s.cancellationFee > 0 && (
          <>
            <dt>Cancellation fee</dt>
            <dd>{vnd(s.cancellationFee)}</dd>
          </>
        )}
        <dt>You earned</dt>
        <dd>
          <strong>{vnd(s.total)}</strong>
        </dd>
      </dl>
    </Card>
  );
}

/** A one-line running total for the driver's idle screen. `refreshKey` re-reads it after a trip. */
export function EarningsLine({ driverId, refreshKey }: { driverId: string; refreshKey: number }) {
  const [earnings, setEarnings] = useState<DriverEarnings | null>(null);
  useEffect(() => {
    let stale = false;
    getDriverEarnings(driverId)
      .then((e) => !stale && setEarnings(e))
      .catch(() => {});
    return () => {
      stale = true;
    };
  }, [driverId, refreshKey]);
  if (!earnings || earnings.trips === 0) return null;
  return (
    <div className="rating-line">
      <span>
        Earned {vnd(earnings.total)} from {earnings.trips} {earnings.trips === 1 ? "trip" : "trips"}
      </span>
    </div>
  );
}
