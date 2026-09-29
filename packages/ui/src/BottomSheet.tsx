import { useState, type ReactNode } from "react";
import { cx } from "./cx";

export interface BottomSheetProps {
  children: ReactNode;
  /** Start collapsed to just the handle + first bit of content. */
  defaultCollapsed?: boolean;
  className?: string;
}

// Positioned absolutely at the bottom of its nearest positioned parent
// (e.g. a full-screen map container), so it overlays the map.
export function BottomSheet({ children, defaultCollapsed = false, className }: BottomSheetProps) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  return (
    <section className={cx("gm-sheet", collapsed && "gm-sheet--collapsed", className)}>
      <button
        className="gm-sheet__handle"
        aria-label={collapsed ? "Expand panel" : "Collapse panel"}
        aria-expanded={!collapsed}
        onClick={() => setCollapsed((c) => !c)}
      />
      <div className="gm-sheet__body">{children}</div>
    </section>
  );
}
