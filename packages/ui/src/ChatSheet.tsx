import { useEffect, useId, useRef, useState, type FormEvent, type KeyboardEvent } from "react";
import { BottomSheet } from "./BottomSheet";
import { Button } from "./Button";
import { cx } from "./cx";

/** One line of the conversation, as the sheet shows it. */
export interface ChatSheetMessage {
  id: string;
  /** True for what this person wrote: shown on the right. */
  mine: boolean;
  text: string;
  /** ISO time. */
  createdAt: string;
  /** Only for messages not yet saved by the server; omit (or "sent") for the rest. */
  status?: "sent" | "sending" | "failed";
}

export interface ChatSheetProps {
  open: boolean;
  /** The heading, e.g. "Chat with driver-2". */
  title?: string;
  /** Oldest first. */
  messages: ChatSheetMessage[];
  /** True while the history is being fetched and there is nothing to show yet. */
  loading?: boolean;
  /** The chat has ended: the composer is replaced by a banner. */
  closed?: boolean;
  /** Sending is paused for now (e.g. rate limited); the composer stays visible. */
  sendDisabled?: boolean;
  /** A short message about the chat itself (a failure, a pause), announced politely. */
  notice?: string;
  /** Longest message in characters; also what the counter counts down from. */
  maxLength?: number;
  /** Receives the trimmed text. The sheet clears its field right away. */
  onSend: (text: string) => void;
  /** The "failed - retry" button of the message with this id. */
  onRetry?: (id: string) => void;
  onClose: () => void;
}

function formatTime(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "" : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

// Counts characters the way the server does (code points, not UTF-16 units).
const lengthOf = (text: string) => [...text].length;

/**
 * The conversation between rider and driver, in a bottom sheet. Presentational: it fetches and
 * sends nothing, the caller passes the messages and handles `onSend`/`onRetry`.
 */
export function ChatSheet({
  open,
  title = "Chat",
  messages,
  loading = false,
  closed = false,
  sendDisabled = false,
  notice,
  maxLength = 500,
  onSend,
  onRetry,
  onClose,
}: ChatSheetProps) {
  const [draft, setDraft] = useState("");
  const listRef = useRef<HTMLUListElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const titleId = useId();
  const counterId = useId();

  // Newest message in view.
  const last = messages[messages.length - 1];
  useEffect(() => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [open, messages.length, last?.status]);

  // Focus the field when the sheet opens so the person can type straight away.
  useEffect(() => {
    if (open && !closed) inputRef.current?.focus();
  }, [open, closed]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  const text = draft.trim();
  const count = lengthOf(draft);
  const tooLong = count > maxLength;
  const canSend = !closed && !sendDisabled && text.length > 0 && !tooLong;

  const submit = () => {
    if (!canSend) return;
    onSend(text);
    setDraft("");
  };

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    submit();
  };

  // The server rejects newlines, so Shift+Enter must not insert one (or send).
  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== "Enter") return;
    e.preventDefault();
    if (!e.shiftKey && !e.nativeEvent.isComposing) submit();
  };

  return (
    <BottomSheet className="gm-chat">
      <div className="gm-chat__panel" role="dialog" aria-labelledby={titleId}>
        <div className="gm-chat__head">
          <h2 className="gm-chat__title" id={titleId}>
            {title}
          </h2>
          <Button variant="ghost" size="sm" onClick={onClose} aria-label="Close chat">
            Close
          </Button>
        </div>

        {loading && messages.length === 0 ? (
          <p className="gm-chat__state" role="status">
            <span className="gm-spinner" aria-hidden="true" /> Loading messages…
          </p>
        ) : messages.length === 0 ? (
          <p className="gm-chat__state">No messages yet. Say hello.</p>
        ) : null}

        <ul
          ref={listRef}
          className={cx("gm-chat__list", messages.length === 0 && "gm-chat__list--empty")}
          role="log"
          aria-live="polite"
          aria-relevant="additions"
          aria-label="Messages"
        >
          {messages.map((m) => (
            <li key={m.id} className={cx("gm-chat__msg", m.mine ? "gm-chat__msg--mine" : "gm-chat__msg--theirs")}>
              <div className={cx("gm-chat__bubble", m.status === "sending" && "gm-chat__bubble--pending", m.status === "failed" && "gm-chat__bubble--failed")}>
                <span className="gm-chat__who">{m.mine ? "You: " : "Them: "}</span>
                {m.text}
              </div>
              <div className="gm-chat__meta">
                {m.status === "sending" ? (
                  <span>Sending…</span>
                ) : m.status === "failed" ? (
                  <button type="button" className="gm-chat__retry" onClick={() => onRetry?.(m.id)}>
                    Failed - retry
                  </button>
                ) : (
                  <time dateTime={m.createdAt}>{formatTime(m.createdAt)}</time>
                )}
              </div>
            </li>
          ))}
        </ul>

        {notice && (
          <p className="gm-chat__notice" role="status">
            {notice}
          </p>
        )}

        {closed ? (
          <p className="gm-chat__banner" role="note">
            This chat has ended
          </p>
        ) : (
          <form className="gm-chat__composer" onSubmit={onSubmit}>
            <input
              ref={inputRef}
              className="gm-chat__input"
              type="text"
              autoComplete="off"
              enterKeyHint="send"
              aria-label="Message"
              aria-describedby={counterId}
              aria-invalid={tooLong || undefined}
              placeholder="Type a message"
              value={draft}
              // Newlines and other control characters are refused by the server, so never keep them (a paste can carry them).
              onChange={(e) => setDraft(e.target.value.replace(/[\u0000-\u001f\u007f]/g, " "))}
              onKeyDown={onKeyDown}
            />
            <span id={counterId} className={cx("gm-chat__count", tooLong && "gm-chat__count--over")}>
              {count}/{maxLength}
            </span>
            <Button type="submit" disabled={!canSend}>
              Send
            </Button>
          </form>
        )}
      </div>
    </BottomSheet>
  );
}

export interface ChatButtonProps {
  /** Messages not yet seen; the badge hides at 0 and shows "9+" past 9. */
  unread?: number;
  onClick: () => void;
  className?: string;
}

/** The "Chat" button that opens a `ChatSheet`, with an unread badge. */
export function ChatButton({ unread = 0, onClick, className }: ChatButtonProps) {
  return (
    <Button
      variant="secondary"
      block
      className={cx("gm-chat-button", className)}
      onClick={onClick}
      aria-label={unread > 0 ? `Chat, ${unread} unread ${unread === 1 ? "message" : "messages"}` : "Chat"}
    >
      Chat
      {unread > 0 && (
        <span className="gm-chat-button__badge" aria-hidden="true">
          {unread > 9 ? "9+" : unread}
        </span>
      )}
    </Button>
  );
}
