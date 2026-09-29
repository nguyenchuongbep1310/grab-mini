import { useState, type KeyboardEvent } from "react";
import { cx } from "./cx";

export interface StarRatingProps {
  /** 0 to 5. Fractions are shown as partly filled stars in read-only mode. */
  value: number;
  /** Makes the rating editable (a radio group). Omit for a read-only display. */
  onChange?: (stars: number) => void;
  size?: "sm" | "md" | "lg";
  /** Names the control for screen readers, e.g. "Rate your driver". */
  label?: string;
  disabled?: boolean;
  className?: string;
}

const STARS = [1, 2, 3, 4, 5];

export function StarRating({ value, onChange, size = "md", label = "Rating", disabled = false, className }: StarRatingProps) {
  const [hover, setHover] = useState(0);

  if (!onChange) {
    const clamped = Math.min(5, Math.max(0, value));
    return (
      <span
        className={cx("gm-stars", "gm-stars--readonly", `gm-stars--${size}`, className)}
        role="img"
        aria-label={`${label}: ${clamped.toFixed(1)} out of 5 stars`}
      >
        <span className="gm-stars__track" aria-hidden="true">★★★★★</span>
        <span className="gm-stars__fill" style={{ width: `${(clamped / 5) * 100}%` }} aria-hidden="true">★★★★★</span>
      </span>
    );
  }

  const shown = hover || value;

  const onKeyDown = (event: KeyboardEvent, star: number) => {
    let next = 0;
    if (event.key === "ArrowRight" || event.key === "ArrowUp") next = Math.min(5, star + 1);
    if (event.key === "ArrowLeft" || event.key === "ArrowDown") next = Math.max(1, star - 1);
    if (!next) return;
    event.preventDefault();
    onChange(next);
    (event.currentTarget.parentElement?.children[next - 1] as HTMLElement | undefined)?.focus();
  };

  return (
    <div
      role="radiogroup"
      aria-label={label}
      className={cx("gm-stars", `gm-stars--${size}`, className)}
      onMouseLeave={() => setHover(0)}
    >
      {STARS.map((star) => (
        <button
          key={star}
          type="button"
          role="radio"
          aria-checked={value === star}
          aria-label={`${star} ${star === 1 ? "star" : "stars"}`}
          // Roving tabindex: the chosen star (or the first, when none is) is the one tab stop.
          tabIndex={value === star || (value === 0 && star === 1) ? 0 : -1}
          disabled={disabled}
          className={cx("gm-stars__star", star <= shown && "gm-stars__star--on")}
          onMouseEnter={() => setHover(star)}
          onClick={() => onChange(star)}
          onKeyDown={(e) => onKeyDown(e, star)}
        >
          ★
        </button>
      ))}
    </div>
  );
}
