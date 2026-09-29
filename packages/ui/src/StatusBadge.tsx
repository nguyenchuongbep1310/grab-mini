import { cx } from "./cx";

type Tone = "success" | "warning" | "danger" | "info" | "neutral";

interface StatusStyle {
  label: string;
  tone: Tone;
  pulse?: boolean;
}

// One place that decides how every domain status looks, so rider and
// driver apps can never disagree about what "IN_PROGRESS" should be.
const STATUS_STYLES: Record<string, StatusStyle> = {
  PENDING: { label: "Finding driver", tone: "warning", pulse: true },
  MATCHED: { label: "Driver found", tone: "info" },
  NO_DRIVER: { label: "No driver available", tone: "danger" },
  ASSIGNED: { label: "Driver on the way", tone: "info", pulse: true },
  IN_PROGRESS: { label: "On trip", tone: "success", pulse: true },
  COMPLETED: { label: "Completed", tone: "neutral" },
  CANCELLED: { label: "Cancelled", tone: "danger" },
  AVAILABLE: { label: "Available", tone: "success" },
  ON_TRIP: { label: "On trip", tone: "success" },
};

export interface StatusBadgeProps {
  status: string;
  className?: string;
}

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const style = STATUS_STYLES[status] ?? { label: status, tone: "neutral" as Tone };
  return (
    <span className={cx("gm-badge", `gm-badge--${style.tone}`, style.pulse && "gm-badge--pulse", className)}>
      {style.label}
    </span>
  );
}
