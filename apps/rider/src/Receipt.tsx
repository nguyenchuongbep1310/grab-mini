import { useEffect, useState } from "react";
import { Card } from "@grab-mini/ui";
import { getTripPayments, type RiderPayment } from "@grab-mini/client";

const vnd = (n: number) => `${n.toLocaleString("vi-VN")}đ`;

export interface ReceiptLines {
  fare: number | null;
  tip: number | null;
  cancellationFee: number | null;
  /** What was actually charged: captured payments only. */
  total: number;
  /** The provider refused the rider's payment method for the fare. */
  declined: boolean;
}

/** Folds a trip's payments into the lines of a receipt. Pure, so it is tested without a server. */
export function summarize(payments: RiderPayment[]): ReceiptLines {
  const captured = (kind: RiderPayment["kind"]) =>
    payments.find((p) => p.kind === kind && p.status === "CAPTURED")?.amount ?? null;
  return {
    fare: captured("FARE"),
    tip: captured("TIP"),
    cancellationFee: captured("CANCELLATION_FEE"),
    total: payments.filter((p) => p.status === "CAPTURED").reduce((sum, p) => sum + p.amount, 0),
    declined: payments.some((p) => p.kind === "FARE" && p.status === "DECLINED"),
  };
}

/**
 * Payment Service settles a trip a moment after it completes (and the tip a
 * moment after the rating), so the receipt polls briefly. `tip` is the tip the
 * rider sent: only when it is above zero is a TIP line worth waiting for.
 */
export function useReceipt(tripId: string | null, rated: boolean, tip: number, intervalMs = 1500, attempts = 10) {
  const [lines, setLines] = useState<ReceiptLines | null>(null);
  const [gaveUp, setGaveUp] = useState(false);

  useEffect(() => {
    if (!tripId) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let left = attempts;
    setGaveUp(false);

    const poll = async () => {
      let next: ReceiptLines | null = null;
      try {
        next = summarize((await getTripPayments(tripId)).payments ?? []);
      } catch {
        // Not settled yet, or Payment Service is briefly unreachable: try again.
      }
      if (stopped) return;
      if (next) setLines((prev) => (prev && prev.total > next!.total ? prev : next));
      const complete = next !== null && (next.fare !== null || next.declined) && (!rated || tip <= 0 || next.tip !== null);
      if (complete) return;
      if (--left <= 0) {
        setGaveUp(true);
        return;
      }
      timer = setTimeout(poll, intervalMs);
    };
    poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [tripId, rated, tip, intervalMs, attempts]);

  return { lines, gaveUp };
}

export interface ReceiptProps {
  tripId: string | null;
  rated: boolean;
  tip: number;
}

/** The receipt a rider sees once a trip completes: fare + tip = total. */
export function Receipt({ tripId, rated, tip }: ReceiptProps) {
  const { lines, gaveUp } = useReceipt(tripId, rated, tip);
  if (!tripId) return null;

  if (!lines || (lines.fare === null && !lines.declined)) {
    // An order without a price (from before pricing) is never charged: say nothing rather than nag.
    return gaveUp ? null : (
      <Card padding="sm">
        <p className="receipt__pending" role="status">Settling your payment…</p>
      </Card>
    );
  }
  return (
    <Card padding="sm">
      <dl className="receipt" aria-label="Receipt">
        <div>
          <dt>Fare</dt>
          <dd>{lines.fare !== null ? vnd(lines.fare) : "Payment declined"}</dd>
        </div>
        {lines.tip !== null && (
          <div>
            <dt>Tip</dt>
            <dd>{vnd(lines.tip)}</dd>
          </div>
        )}
        <div className="receipt__total">
          <dt>Total</dt>
          <dd>{vnd(lines.total)}</dd>
        </div>
      </dl>
    </Card>
  );
}
