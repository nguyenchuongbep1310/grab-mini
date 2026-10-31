import { useEffect, useId, type ReactNode } from "react";
import { BottomSheet } from "./BottomSheet";
import { Button } from "./Button";

export interface FormSheetProps {
  open: boolean;
  title: string;
  children: ReactNode;
  onClose: () => void;
}

/** A bottom sheet with a heading and a Close button, for a short form over the map (a profile, say). Escape closes it. */
export function FormSheet({ open, title, children, onClose }: FormSheetProps) {
  const titleId = useId();

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <BottomSheet className="gm-form-sheet">
      <div className="gm-form-sheet__panel" role="dialog" aria-labelledby={titleId}>
        <div className="gm-form-sheet__head">
          <h2 className="gm-form-sheet__title" id={titleId}>
            {title}
          </h2>
          <Button variant="ghost" size="sm" onClick={onClose} aria-label={`Close ${title.toLowerCase()}`}>
            Close
          </Button>
        </div>
        {children}
      </div>
    </BottomSheet>
  );
}
