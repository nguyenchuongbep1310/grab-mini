import type { HTMLAttributes } from "react";
import { cx } from "./cx";

export interface CardProps extends HTMLAttributes<HTMLDivElement> {
  padding?: "none" | "sm" | "md" | "lg";
  elevated?: boolean;
}

export function Card({ padding = "md", elevated = false, className, ...rest }: CardProps) {
  return <div className={cx("gm-card", `gm-card--pad-${padding}`, elevated && "gm-card--elevated", className)} {...rest} />;
}
