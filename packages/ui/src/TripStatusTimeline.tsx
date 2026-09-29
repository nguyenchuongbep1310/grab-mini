import { cx } from "./cx";

export interface TimelineStep {
  key: string;
  label: string;
  hint?: string;
}

export interface TripStatusTimelineProps {
  steps: TimelineStep[];
  /** key of the step currently in progress; steps before it are done. */
  currentKey: string;
  /** Mark the current step as failed (e.g. cancelled / no driver). */
  failed?: boolean;
  className?: string;
}

export function TripStatusTimeline({ steps, currentKey, failed = false, className }: TripStatusTimelineProps) {
  const currentIndex = Math.max(
    0,
    steps.findIndex((s) => s.key === currentKey),
  );

  return (
    <ol className={cx("gm-timeline", className)}>
      {steps.map((step, i) => {
        const state =
          i < currentIndex ? "done" : i === currentIndex ? (failed ? "failed" : "current") : "upcoming";
        // The last step being "current" means the flow is finished: show it as done.
        const effective = state === "current" && i === steps.length - 1 ? "done" : state;
        return (
          <li key={step.key} className={cx("gm-timeline__step", `gm-timeline__step--${effective}`)} aria-current={state === "current" ? "step" : undefined}>
            <span className="gm-timeline__dot" aria-hidden="true">
              {effective === "done" ? "✓" : effective === "failed" ? "!" : null}
            </span>
            <div>
              <div className="gm-timeline__label">{step.label}</div>
              {step.hint && effective !== "upcoming" && <div className="gm-timeline__hint">{step.hint}</div>}
            </div>
          </li>
        );
      })}
    </ol>
  );
}
