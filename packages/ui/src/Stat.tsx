import type { ReactNode } from "react";
import { cx } from "./cx";

export interface StatProps {
  label: string;
  value: ReactNode;
  /** Small line under the value, e.g. a unit or a comparison. */
  hint?: string;
  /** Colors the value; defaults to the normal text color. */
  tone?: "default" | "success" | "warning" | "danger";
  className?: string;
}

// A single headline number, for dashboards.
export function Stat({ label, value, hint, tone = "default", className }: StatProps) {
  return (
    <div className={cx("gm-stat", className)}>
      <div className="gm-stat__label">{label}</div>
      <div className={cx("gm-stat__value", `gm-stat__value--${tone}`)}>{value}</div>
      {hint && <div className="gm-stat__hint">{hint}</div>}
    </div>
  );
}
