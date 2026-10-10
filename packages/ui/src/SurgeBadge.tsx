import { cx } from "./cx";

export interface SurgeBadgeProps {
  /** Surge multiplier; nothing is shown at 1.0 (or below). */
  multiplier: number;
  /** Text after the multiplier. */
  label?: string;
  className?: string;
}

// "x1.4 high demand": renders nothing when there is no surge, so callers can
// always drop it in next to a price.
export function SurgeBadge({ multiplier, label = "high demand", className }: SurgeBadgeProps) {
  if (!(multiplier > 1)) return null;
  return (
    <span className={cx("gm-surge", className)} role="status">
      <span className="gm-surge__mult">x{multiplier.toFixed(1)}</span> {label}
    </span>
  );
}
