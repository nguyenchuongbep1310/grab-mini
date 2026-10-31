import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import type { ChatNotification } from "@grab-mini/events";
import {
  CHAT_MAX_LENGTH,
  chatErrorOf,
  chatMessageFromPush,
  chatRetryAfterOf,
  listChatMessages,
  sendChatMessage,
  type ChatErrorCode,
  type ChatHistory,
  type ChatMessage,
} from "./api";
import type { SocketState } from "./useNotificationSocket";

/** True for a `chat.message.sent` push. Those are not in `NotificationMessage` on purpose: the app's trip reducers never see them. */
export function isChatPush(message: unknown): message is ChatNotification {
  const m = message as { type?: unknown; payload?: unknown } | null;
  return !!m && m.type === "chat.message.sent" && typeof m.payload === "object" && m.payload !== null;
}

/** One line of the conversation, ready to show (it matches `ChatSheet`'s message type). */
export interface ChatItem {
  /** The server's message id; `pending-<key>` for a message the server has not confirmed yet. */
  id: string;
  mine: boolean;
  text: string;
  /** ISO time. */
  createdAt: string;
  status: "sent" | "sending" | "failed";
}

export interface UseChatOptions {
  /** The signed-in user's id, to tell their messages from the other person's. */
  me: string;
  /** False while there is no chat to show (no trip, or a phase without one); turning it off clears everything. */
  enabled: boolean;
  /**
   * The state of the notification socket. When it comes back after a drop, the history is reloaded:
   * pushes are best-effort and the REST history is how a missed one is recovered (ADR 0008).
   */
  socketState?: SocketState;
}

export interface UseChat {
  messages: ChatItem[];
  /** The first fetch of the history is under way. */
  loading: boolean;
  /** The chat is read-only (the grace period after the trip is over). */
  closed: boolean;
  closesAt: string | null;
  /** Sending is paused after a rate limit; `notice` says for how long. */
  sendBlocked: boolean;
  /** A short sentence about the last problem (or the pause), "" for none. */
  notice: string;
  /** Messages from the other person that arrived while the sheet was closed. */
  unread: number;
  sheetOpen: boolean;
  openSheet: () => void;
  closeSheet: () => void;
  /** Sends (or ignores, if empty, too long, closed or paused). The message shows at once as "sending". */
  send: (text: string) => void;
  /** Sends a failed message again with the same Idempotency-Key, so a send the server already saved is not doubled. */
  retry: (id: string) => void;
  /**
   * Feed it every push from the notification socket; returns true when it was a chat push (so the
   * caller can skip its own handling). A push for another trip is dropped.
   */
  receive: (message: unknown) => boolean;
}

interface Outgoing {
  key: string;
  text: string;
  createdAt: string;
  status: "sending" | "failed";
}

interface State {
  messages: ChatMessage[];
  outbox: Outgoing[];
  loading: boolean;
  loaded: boolean;
  loadFailed: boolean;
  closed: boolean;
  closesAt: string | null;
  notice: string;
  unread: number;
  sheetOpen: boolean;
}

const initial: State = {
  messages: [],
  outbox: [],
  loading: false,
  loaded: false,
  loadFailed: false,
  closed: false,
  closesAt: null,
  notice: "",
  unread: 0,
  sheetOpen: false,
};

type Action =
  | { type: "reset" }
  | { type: "loadStarted" }
  | { type: "loaded"; history: ChatHistory; me: string }
  | { type: "loadFailed"; notice: string }
  | { type: "pushed"; message: ChatMessage; me: string }
  | { type: "sendStarted"; out: Outgoing }
  | { type: "sent"; key: string; message: ChatMessage }
  | { type: "sendFailed"; key: string; notice: string }
  | { type: "sendDropped"; key: string; notice: string; closed?: boolean }
  | { type: "retryStarted"; key: string }
  | { type: "notice"; notice: string }
  | { type: "sheet"; open: boolean };

// "...:00.5Z" and "...:00.123456Z" do not sort as strings, so compare the instant, then the fraction.
function compareMessages(a: ChatMessage, b: ChatMessage): number {
  const ms = Date.parse(a.createdAt) - Date.parse(b.createdAt);
  if (ms) return ms;
  const frac = (iso: string) => (/\.(\d+)/.exec(iso)?.[1] ?? "").padEnd(9, "0");
  const fa = frac(a.createdAt);
  const fb = frac(b.createdAt);
  if (fa !== fb) return fa < fb ? -1 : 1;
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/** The union of both lists by message id (a push and a reload can carry the same message), oldest first. */
function merge(existing: ChatMessage[], incoming: ChatMessage[]): ChatMessage[] {
  const byId = new Map(existing.map((m) => [m.id, m]));
  for (const m of incoming) byId.set(m.id, m);
  return [...byId.values()].sort(compareMessages);
}

function reduce(state: State, action: Action): State {
  switch (action.type) {
    case "reset":
      return initial;
    case "loadStarted":
      return { ...state, loading: !state.loaded };
    case "loaded": {
      const { history, me } = action;
      const known = new Set(state.messages.map((m) => m.id));
      // The first load is what was said before this page opened, not news. Later ones can be.
      const news = state.loaded && !state.sheetOpen ? history.messages.filter((m) => !known.has(m.id) && m.senderId !== me).length : 0;
      return {
        ...state,
        messages: merge(state.messages, history.messages),
        loading: false,
        loaded: true,
        loadFailed: false,
        notice: state.loadFailed ? "" : state.notice,
        closed: !history.open,
        closesAt: history.closesAt,
        unread: state.unread + news,
      };
    }
    case "loadFailed":
      return { ...state, loading: false, loadFailed: true, notice: action.notice };
    case "pushed": {
      if (state.messages.some((m) => m.id === action.message.id)) return state;
      const theirs = action.message.senderId !== action.me;
      return {
        ...state,
        messages: merge(state.messages, [action.message]),
        unread: theirs && !state.sheetOpen ? state.unread + 1 : state.unread,
      };
    }
    case "sendStarted":
      return { ...state, outbox: [...state.outbox, action.out], notice: "" };
    case "sent":
      return {
        ...state,
        messages: merge(state.messages, [action.message]),
        outbox: state.outbox.filter((o) => o.key !== action.key),
      };
    case "sendFailed":
      return {
        ...state,
        outbox: state.outbox.map((o) => (o.key === action.key ? { ...o, status: "failed" } : o)),
        notice: action.notice,
      };
    case "sendDropped":
      return {
        ...state,
        outbox: state.outbox.filter((o) => o.key !== action.key),
        notice: action.notice,
        closed: state.closed || !!action.closed,
      };
    case "retryStarted":
      return {
        ...state,
        outbox: state.outbox.map((o) => (o.key === action.key ? { ...o, status: "sending" } : o)),
        notice: "",
      };
    case "notice":
      return { ...state, notice: action.notice };
    case "sheet":
      return { ...state, sheetOpen: action.open, unread: action.open ? 0 : state.unread };
  }
}

/** Why a message that will not go through (retrying cannot help) was not sent. */
const REFUSED: Partial<Record<ChatErrorCode, string>> = {
  invalid_text: "That message can't be sent. Use 1 to 500 characters on one line.",
  invalid_request: "That message can't be sent.",
  not_participant: "You're not part of this trip's chat.",
  no_recipient: "There's nobody to send this to yet.",
  key_mismatch: "That message can't be sent. Please type it again.",
};

const lengthOf = (text: string) => [...text].length;

function newKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}-${Math.random().toString(36).slice(2)}`;
}

/**
 * The chat of one trip, for the rider or the driver. Loads the history by REST, appends pushes
 * from the notification socket (feed them to `receive`), sends with an Idempotency-Key and shows
 * the message at once ("sending", then confirmed or "failed" with a retry that reuses the key).
 *
 * Message text is personal data: nothing here logs it, or the errors that might carry it.
 */
export function useChat(tripId: string | null, { me, enabled, socketState }: UseChatOptions): UseChat {
  const [state, dispatch] = useReducer(reduce, initial);
  const [cooldown, setCooldown] = useState(0);
  const active = enabled && !!tripId ? tripId : null;

  // The latest values for callbacks that outlive a render (responses, socket pushes).
  const latest = useRef({ active, me, state, cooldown });
  useEffect(() => {
    latest.current = { active, me, state, cooldown };
  });
  // Bumped on every trip change and every load, so a slow answer for an old one is ignored.
  const loadSeq = useRef(0);

  const load = useCallback(
    async (tripId: string) => {
      const seq = ++loadSeq.current;
      dispatch({ type: "loadStarted" });
      try {
        const history = await listChatMessages(tripId, { limit: 100 });
        if (seq === loadSeq.current) dispatch({ type: "loaded", history, me: latest.current.me });
      } catch {
        if (seq === loadSeq.current) dispatch({ type: "loadFailed", notice: "Couldn't load the messages." });
      }
    },
    [],
  );

  // A new trip (or none) starts from nothing.
  useEffect(() => {
    loadSeq.current++;
    dispatch({ type: "reset" });
    setCooldown(0);
    if (active) void load(active);
  }, [active, load]);

  // Back after a drop: a push may have been missed meanwhile, so ask the server.
  const wasClosed = useRef(false);
  useEffect(() => {
    if (socketState === "closed") wasClosed.current = true;
    if (socketState === "open" && wasClosed.current) {
      wasClosed.current = false;
      if (active) void load(active);
    }
  }, [socketState, active, load]);

  // The pause after a rate limit counts down once a second.
  useEffect(() => {
    if (cooldown <= 0) return;
    const timer = setTimeout(() => setCooldown((c) => c - 1), 1000);
    return () => clearTimeout(timer);
  }, [cooldown]);

  const attempt = useCallback(async (tripId: string, out: Outgoing) => {
    const stillHere = () => latest.current.active === tripId;
    try {
      const message = await sendChatMessage(tripId, out.text, { idempotencyKey: out.key });
      if (stillHere()) dispatch({ type: "sent", key: out.key, message });
    } catch (err) {
      if (!stillHere()) return;
      const code = chatErrorOf(err);
      if (code === "chat_closed") {
        dispatch({ type: "sendDropped", key: out.key, notice: "This chat has ended.", closed: true });
      } else if (code && REFUSED[code]) {
        dispatch({ type: "sendDropped", key: out.key, notice: REFUSED[code] });
      } else if (code === "rate_limited") {
        setCooldown(Math.ceil(chatRetryAfterOf(err) ?? 1));
        dispatch({ type: "sendFailed", key: out.key, notice: "" });
      } else {
        // The network, a 5xx, or a trip chat does not know yet: the message may still go through later.
        dispatch({ type: "sendFailed", key: out.key, notice: "Couldn't send. Tap retry." });
      }
    }
  }, []);

  const send = useCallback(
    (raw: string) => {
      const { active: tripId, state, cooldown } = latest.current;
      const text = raw.trim();
      if (!tripId || !text || state.closed || cooldown > 0) return;
      if (lengthOf(text) > CHAT_MAX_LENGTH) {
        dispatch({ type: "notice", notice: `Messages can have up to ${CHAT_MAX_LENGTH} characters.` });
        return;
      }
      const out: Outgoing = { key: newKey(), text, createdAt: new Date().toISOString(), status: "sending" };
      dispatch({ type: "sendStarted", out });
      void attempt(tripId, out);
    },
    [attempt],
  );

  const retry = useCallback(
    (id: string) => {
      const { active: tripId, state, cooldown } = latest.current;
      const out = state.outbox.find((o) => `pending-${o.key}` === id);
      if (!tripId || !out || out.status !== "failed" || state.closed || cooldown > 0) return;
      dispatch({ type: "retryStarted", key: out.key });
      void attempt(tripId, out); // the same key: if the first try did reach the server, this returns that message
    },
    [attempt],
  );

  const receive = useCallback((message: unknown) => {
    if (!isChatPush(message)) return false;
    const { active, me } = latest.current;
    if (active && message.payload.tripId === active) {
      dispatch({ type: "pushed", message: chatMessageFromPush(message.payload), me });
    }
    return true;
  }, []);

  const openSheet = useCallback(() => {
    dispatch({ type: "sheet", open: true });
    const { active, state } = latest.current;
    if (active && state.loadFailed) void load(active);
  }, [load]);
  const closeSheet = useCallback(() => dispatch({ type: "sheet", open: false }), []);

  const messages: ChatItem[] = [
    ...state.messages.map((m): ChatItem => ({ id: m.id, mine: m.senderId === me, text: m.text, createdAt: m.createdAt, status: "sent" })),
    ...state.outbox.map((o): ChatItem => ({ id: `pending-${o.key}`, mine: true, text: o.text, createdAt: o.createdAt, status: o.status })),
  ];

  return {
    messages,
    loading: state.loading,
    closed: state.closed,
    closesAt: state.closesAt,
    sendBlocked: cooldown > 0,
    notice: cooldown > 0 ? `You're sending messages too fast. Try again in ${cooldown}s.` : state.notice,
    unread: state.unread,
    sheetOpen: state.sheetOpen,
    openSheet,
    closeSheet,
    send,
    retry,
    receive,
  };
}
