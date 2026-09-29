import type { ButtonHTMLAttributes } from "react";
import { cx } from "./cx";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "ghost" | "danger";
  size?: "sm" | "md" | "lg";
  block?: boolean;
  loading?: boolean;
}

export function Button({
  variant = "primary",
  size = "md",
  block = false,
  loading = false,
  disabled,
  className,
  children,
  type = "button",
  ...rest
}: ButtonProps) {
  return (
    <button
      type={type}
      className={cx("gm-btn", `gm-btn--${variant}`, `gm-btn--${size}`, block && "gm-btn--block", className)}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      {...rest}
    >
      {loading && <span className="gm-spinner" aria-hidden="true" />}
      {children}
    </button>
  );
}
