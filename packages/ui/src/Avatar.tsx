import { cx } from "./cx";

export interface AvatarProps {
  name: string;
  src?: string;
  size?: "sm" | "md" | "lg";
  className?: string;
}

function initials(name: string): string {
  const parts = name.trim().split(/[\s-_]+/).filter(Boolean);
  if (parts.length === 0) return "?";
  const first = parts[0][0];
  const last = parts.length > 1 ? parts[parts.length - 1][0] : "";
  return (first + last).toUpperCase();
}

export function Avatar({ name, src, size = "md", className }: AvatarProps) {
  return (
    <span className={cx("gm-avatar", `gm-avatar--${size}`, className)} role="img" aria-label={name}>
      {src ? <img src={src} alt="" /> : initials(name)}
    </span>
  );
}
