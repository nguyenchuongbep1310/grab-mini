import { cx } from "./cx";

export interface EtaBadgeProps {
  /** Travel time in seconds. */
  durationSec: number;
  /** Route length in km; shown as "3.2 km" before the time when given. */
  distanceKm?: number;
  /** Text before the time, e.g. "Driver arrives in". */
  label?: string;
  className?: string;
}

/** Whole minutes for display, never less than 1: "about 0 min" reads as a bug. */
export function formatEta(durationSec: number): string {
  return `about ${Math.max(1, Math.round(durationSec / 60))} min`;
}

// "3.2 km · about 8 min" or "Driver arrives in about 3 min". Renders nothing for a
// missing or invalid estimate, so callers can drop it in and pass a failed lookup as is.
export function EtaBadge({ durationSec, distanceKm, label, className }: EtaBadgeProps) {
  if (!Number.isFinite(durationSec) || durationSec < 0) return null;
  const parts = [
    label,
    distanceKm !== undefined && Number.isFinite(distanceKm) ? `${distanceKm.toFixed(1)} km ·` : undefined,
    formatEta(durationSec),
  ].filter(Boolean);
  return <span className={cx("gm-eta", className)}>{parts.join(" ")}</span>;
}
