import { useState } from "react";

export const MAX_COMMENT = 500;
export const MAX_TIP = 1_000_000;
const PRESETS = [0, 10_000, 20_000, 50_000];

const vnd = (n: number) => `${n.toLocaleString("vi-VN")}đ`;

export interface TipPickerProps {
  comment: string;
  onComment: (value: string) => void;
  /** Whole VND, 0 for no tip. */
  tip: number;
  onTip: (value: number) => void;
  disabled?: boolean;
}

/** Optional comment and tip for a rating. The tip is only recorded: no money moves. */
export function TipPicker({ comment, onComment, tip, onTip, disabled }: TipPickerProps) {
  const [custom, setCustom] = useState(false);
  const isPreset = PRESETS.includes(tip);

  return (
    <div className="tip-picker">
      <label className="tip-picker__label" htmlFor="rating-comment">
        Comment (optional)
      </label>
      <textarea
        id="rating-comment"
        className="tip-picker__comment"
        rows={3}
        value={comment}
        maxLength={MAX_COMMENT}
        disabled={disabled}
        placeholder="Tell us about your ride"
        onChange={(e) => onComment(e.target.value)}
      />
      <div className="tip-picker__counter" aria-live="polite">
        {[...comment].length}/{MAX_COMMENT}
      </div>

      <div className="tip-picker__label">Add a tip</div>
      <div className="tip-picker__chips" role="group" aria-label="Tip amount">
        {PRESETS.map((amount) => (
          <button
            key={amount}
            type="button"
            className="tip-chip"
            aria-pressed={!custom && tip === amount}
            disabled={disabled}
            onClick={() => {
              setCustom(false);
              onTip(amount);
            }}
          >
            {amount === 0 ? "No tip" : vnd(amount)}
          </button>
        ))}
        <button
          type="button"
          className="tip-chip"
          aria-pressed={custom || !isPreset}
          disabled={disabled}
          onClick={() => setCustom(true)}
        >
          Custom
        </button>
      </div>
      {(custom || !isPreset) && (
        <input
          className="tip-picker__custom"
          type="number"
          inputMode="numeric"
          min={0}
          max={MAX_TIP}
          step={1000}
          value={tip || ""}
          disabled={disabled}
          placeholder="Amount in VND"
          aria-label="Custom tip in VND"
          onChange={(e) => {
            const n = Math.floor(Number(e.target.value));
            onTip(Number.isFinite(n) ? Math.min(Math.max(n, 0), MAX_TIP) : 0);
          }}
        />
      )}
    </div>
  );
}

export interface RatingSummaryProps {
  comment: string;
  /** Whole VND, 0 for no tip. */
  tip: number;
}

/** Read-only recap of the tip and note a rider sent with their rating. */
export function RatingSummary({ comment, tip }: RatingSummaryProps) {
  const note = comment.trim();
  return (
    <dl className="rating-summary">
      <div>
        <dt>Tip</dt>
        <dd>{tip > 0 ? vnd(tip) : "No tip"}</dd>
      </div>
      {note && (
        <div>
          <dt>Your note</dt>
          <dd className="rating-summary__note">{note}</dd>
        </div>
      )}
    </dl>
  );
}
