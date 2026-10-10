import { useEffect, useId, useRef, useState } from "react";
import { Button } from "./Button";

export interface CancelDialogProps {
  open: boolean;
  title: string;
  description?: string;
  /** Quick reasons to pick from; a free-text "Other" is always offered. */
  reasons: string[];
  confirmLabel?: string;
  dismissLabel?: string;
  /** Disables the buttons while the cancel request is in flight. */
  loading?: boolean;
  /** Called with the chosen or typed reason, or "" if none was given. */
  onConfirm: (reason: string) => void;
  onDismiss: () => void;
}

const OTHER = "__other__";
export const MAX_REASON_LENGTH = 200;

/** The reason to send for a picker choice and the text typed under "Other". */
export function reasonFor(choice: string, other: string): string {
  const text = choice === OTHER ? other : choice;
  return text.trim().slice(0, MAX_REASON_LENGTH);
}

// A confirm dialog for cancelling a ride: asks "are you sure?" and, optionally, why.
export function CancelDialog({
  open,
  title,
  description,
  reasons,
  confirmLabel = "Cancel ride",
  dismissLabel = "Keep ride",
  loading = false,
  onConfirm,
  onDismiss,
}: CancelDialogProps) {
  const [choice, setChoice] = useState("");
  const [other, setOther] = useState("");
  const titleId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);

  // Start fresh each time it opens, with focus on the safe choice.
  useEffect(() => {
    if (!open) return;
    setChoice("");
    setOther("");
    dialogRef.current?.querySelector<HTMLButtonElement>(".gm-dialog__actions button")?.focus();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !loading) onDismiss();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, loading, onDismiss]);

  if (!open) return null;

  return (
    <div className="gm-dialog-backdrop" onClick={() => !loading && onDismiss()}>
      <div
        ref={dialogRef}
        className="gm-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="gm-dialog__title" id={titleId}>
          {title}
        </h2>
        {description && <p className="gm-dialog__text">{description}</p>}

        <fieldset className="gm-dialog__reasons" disabled={loading}>
          <legend>Reason (optional)</legend>
          {[...reasons, OTHER].map((r) => (
            <label key={r} className="gm-dialog__reason">
              <input type="radio" name="cancel-reason" checked={choice === r} onChange={() => setChoice(r)} />
              <span>{r === OTHER ? "Other" : r}</span>
            </label>
          ))}
          {choice === OTHER && (
            <textarea
              className="gm-dialog__other"
              rows={2}
              maxLength={MAX_REASON_LENGTH}
              placeholder="Tell us more"
              value={other}
              onChange={(e) => setOther(e.target.value)}
            />
          )}
        </fieldset>

        <div className="gm-dialog__actions">
          <Button variant="secondary" disabled={loading} onClick={onDismiss}>
            {dismissLabel}
          </Button>
          <Button variant="danger" loading={loading} onClick={() => onConfirm(reasonFor(choice, other))}>
            {confirmLabel}
          </Button>
        </div>
      </div>
    </div>
  );
}
