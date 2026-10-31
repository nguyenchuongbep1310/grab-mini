import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ChatMessageSent } from "@grab-mini/events";
import * as api from "./api";
import { ApiError, type ChatHistory, type ChatMessage } from "./api";
import { isChatPush, useChat, type UseChatOptions } from "./useChat";
import type { SocketState } from "./useNotificationSocket";

vi.mock("./api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./api")>()),
  listChatMessages: vi.fn(),
  sendChatMessage: vi.fn(),
}));

const list = vi.mocked(api.listChatMessages);
const sendApi = vi.mocked(api.sendChatMessage);

const msg = (id: string, senderId: string, text: string, at: string): ChatMessage => ({
  id,
  tripId: "trip-1",
  senderId,
  senderRole: senderId === "rider-1" ? "rider" : "driver",
  text,
  createdAt: `2026-01-01T08:${at}Z`,
});

const history = (messages: ChatMessage[], extra: Partial<ChatHistory> = {}): ChatHistory => ({
  messages: [...messages].reverse(), // the API answers newest first
  nextBefore: null,
  tripStatus: "ASSIGNED",
  open: true,
  closesAt: null,
  ...extra,
});

const push = (m: ChatMessage, tripId = m.tripId) => ({
  type: "chat.message.sent" as const,
  payload: {
    eventId: "e-" + m.id,
    occurredAt: m.createdAt,
    messageId: m.id,
    tripId,
    senderId: m.senderId,
    senderRole: m.senderRole,
    recipientId: m.senderId === "rider-1" ? "driver-1" : "rider-1",
    recipientRole: m.senderRole === "rider" ? "driver" : "rider",
    text: m.text,
  } as ChatMessageSent,
});

const hello = msg("m1", "driver-1", "I am at the gate", "00:00.5");

function setup(opts: Partial<UseChatOptions> = {}, tripId: string | null = "trip-1") {
  return renderHook((props: { tripId: string | null; opts: UseChatOptions }) => useChat(props.tripId, props.opts), {
    initialProps: { tripId, opts: { me: "rider-1", enabled: true, ...opts } },
  });
}

/** Lets the pending promises of the mocked client settle. */
const settle = () => act(async () => {});

beforeEach(() => {
  list.mockReset();
  sendApi.mockReset();
  list.mockResolvedValue(history([]));
});
afterEach(() => vi.useRealTimers());

describe("isChatPush", () => {
  it("recognises chat pushes only", () => {
    expect(isChatPush(push(hello))).toBe(true);
    expect(isChatPush({ type: "trip.rated", payload: {} })).toBe(false);
    expect(isChatPush({ type: "chat.message.sent" })).toBe(false);
    expect(isChatPush(null)).toBe(false);
    expect(isChatPush("chat.message.sent")).toBe(false);
  });
});

describe("useChat history", () => {
  it("loads the history oldest first and marks mine", async () => {
    list.mockResolvedValue(history([hello, msg("m2", "rider-1", "Coming", "01:00")]));
    const { result } = setup();
    expect(result.current.loading).toBe(true);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(list).toHaveBeenCalledWith("trip-1", { limit: 100 });
    expect(result.current.messages.map((m) => [m.id, m.mine, m.status])).toEqual([
      ["m1", false, "sent"],
      ["m2", true, "sent"],
    ]);
    // What was said before this page opened is not news.
    expect(result.current.unread).toBe(0);
    expect(result.current.closed).toBe(false);
  });

  it("is read-only when the server says the chat is closed", async () => {
    list.mockResolvedValue(history([hello], { open: false, closesAt: "2026-01-01T09:00:00Z", tripStatus: "COMPLETED" }));
    const { result } = setup();
    await waitFor(() => expect(result.current.closed).toBe(true));
    expect(result.current.closesAt).toBe("2026-01-01T09:00:00Z");
    act(() => result.current.send("too late"));
    expect(sendApi).not.toHaveBeenCalled();
  });

  it("does nothing without a trip or while disabled, and clears when turned off", async () => {
    list.mockResolvedValue(history([hello]));
    const { result, rerender } = setup({ enabled: false });
    await settle();
    expect(list).not.toHaveBeenCalled();
    rerender({ tripId: null, opts: { me: "rider-1", enabled: true } });
    await settle();
    expect(list).not.toHaveBeenCalled();

    rerender({ tripId: "trip-1", opts: { me: "rider-1", enabled: true } });
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    rerender({ tripId: "trip-1", opts: { me: "rider-1", enabled: false } });
    expect(result.current.messages).toEqual([]);
  });

  it("starts from nothing on another trip and ignores a late answer for the old one", async () => {
    let resolveOld: (h: ChatHistory) => void = () => {};
    list.mockImplementationOnce(() => new Promise((r) => (resolveOld = r)));
    const { result, rerender } = setup();
    list.mockResolvedValueOnce(history([msg("n1", "driver-9", "new trip", "05:00")]));
    rerender({ tripId: "trip-2", opts: { me: "rider-1", enabled: true } });
    await waitFor(() => expect(result.current.messages.map((m) => m.id)).toEqual(["n1"]));
    await act(async () => resolveOld(history([hello])));
    expect(result.current.messages.map((m) => m.id)).toEqual(["n1"]);
  });

  it("says so when the history can't be loaded, and loads again when the sheet opens", async () => {
    list.mockRejectedValueOnce(new ApiError("unknown trip", 404, { code: "unknown_trip" }));
    const { result } = setup();
    await waitFor(() => expect(result.current.notice).toBe("Couldn't load the messages."));
    list.mockResolvedValueOnce(history([hello]));
    act(() => result.current.openSheet());
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    expect(result.current.notice).toBe("");
  });
});

describe("useChat pushes", () => {
  it("appends a push for this trip, ignores another trip's, and says it handled both", async () => {
    const { result } = setup();
    await settle();
    let handled = false;
    act(() => {
      handled = result.current.receive(push(hello));
    });
    expect(handled).toBe(true);
    act(() => {
      result.current.receive(push(msg("x", "driver-1", "elsewhere", "01:00"), "trip-OTHER"));
    });
    expect(result.current.messages.map((m) => m.id)).toEqual(["m1"]);
    expect(result.current.receive({ type: "order.matched", payload: {} })).toBe(false);
  });

  it("never duplicates: a push then a reload, a reload then a push, or the same push twice", async () => {
    const { result } = setup();
    await settle();
    act(() => void result.current.receive(push(hello)));
    act(() => void result.current.receive(push(hello)));
    expect(result.current.messages).toHaveLength(1);

    // The reload (reconnect) returns the pushed message again, plus one the socket missed.
    const missed = msg("m2", "driver-1", "Are you there?", "01:00");
    const { result: r2, rerender } = setup({ socketState: "open" });
    await settle();
    list.mockResolvedValueOnce(history([hello, missed]));
    rerender({ tripId: "trip-1", opts: { me: "rider-1", enabled: true, socketState: "closed" } });
    act(() => void r2.current.receive(push(hello)));
    rerender({ tripId: "trip-1", opts: { me: "rider-1", enabled: true, socketState: "open" } });
    await waitFor(() => expect(r2.current.messages.map((m) => m.id)).toEqual(["m1", "m2"]));
    act(() => void r2.current.receive(push(missed)));
    expect(r2.current.messages.map((m) => m.id)).toEqual(["m1", "m2"]);
    // m1 counted once (push), m2 once (reload).
    expect(r2.current.unread).toBe(2);
  });

  it("orders by time even when the fractions differ in length", async () => {
    const { result } = setup();
    await settle();
    act(() => {
      result.current.receive(push(msg("b", "driver-1", "second", "00:00.5")));
      result.current.receive(push(msg("a", "driver-1", "first", "00:00.123456")));
      result.current.receive(push(msg("c", "driver-1", "third", "00:00.500001")));
    });
    expect(result.current.messages.map((m) => m.id)).toEqual(["a", "b", "c"]);
  });

  it("counts unread while the sheet is closed, and not while it is open", async () => {
    const { result } = setup();
    await settle();
    act(() => void result.current.receive(push(hello)));
    expect(result.current.unread).toBe(1);
    act(() => void result.current.receive(push(msg("m2", "driver-1", "again", "01:00"))));
    expect(result.current.unread).toBe(2);

    act(() => result.current.openSheet());
    expect(result.current.sheetOpen).toBe(true);
    expect(result.current.unread).toBe(0);
    act(() => void result.current.receive(push(msg("m3", "driver-1", "while open", "02:00"))));
    expect(result.current.unread).toBe(0);

    act(() => result.current.closeSheet());
    act(() => void result.current.receive(push(msg("m4", "driver-1", "closed again", "03:00"))));
    expect(result.current.unread).toBe(1);
  });

  it("does not count my own message as unread", async () => {
    const { result } = setup();
    await settle();
    act(() => void result.current.receive(push(msg("m1", "rider-1", "from my other tab", "00:00"))));
    expect(result.current.unread).toBe(0);
    expect(result.current.messages[0].mine).toBe(true);
  });
});

describe("useChat reconnect", () => {
  it("reloads the history when the socket comes back, not on the first connect", async () => {
    const { result, rerender } = setup({ socketState: "connecting" });
    await settle();
    expect(list).toHaveBeenCalledTimes(1);
    const at = (socketState: SocketState) => rerender({ tripId: "trip-1", opts: { me: "rider-1", enabled: true, socketState } });

    at("open");
    await settle();
    expect(list).toHaveBeenCalledTimes(1);

    at("closed");
    at("connecting");
    await settle();
    expect(list).toHaveBeenCalledTimes(1);

    list.mockResolvedValueOnce(history([hello]));
    at("open");
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    expect(list).toHaveBeenCalledTimes(2);
    expect(result.current.unread).toBe(1);
  });
});

describe("useChat sending", () => {
  it("shows the message at once, then swaps in the server's", async () => {
    let resolve: (m: ChatMessage) => void = () => {};
    sendApi.mockImplementationOnce(() => new Promise((r) => (resolve = r)));
    const { result } = setup();
    await settle();

    act(() => result.current.send("  On my way  "));
    expect(result.current.messages).toMatchObject([{ mine: true, text: "On my way", status: "sending" }]);
    expect(sendApi).toHaveBeenCalledWith("trip-1", "On my way", { idempotencyKey: expect.any(String) });

    await act(async () => resolve(msg("s1", "rider-1", "On my way", "04:00")));
    expect(result.current.messages).toMatchObject([{ id: "s1", mine: true, text: "On my way", status: "sent" }]);
  });

  it("ignores empty text and text over 500 characters", async () => {
    const { result } = setup();
    await settle();
    act(() => result.current.send("   "));
    expect(sendApi).not.toHaveBeenCalled();
    act(() => result.current.send("x".repeat(501)));
    expect(sendApi).not.toHaveBeenCalled();
    expect(result.current.notice).toBe("Messages can have up to 500 characters.");
    expect(result.current.messages).toEqual([]);
  });

  it("counts characters, not UTF-16 units, against the limit", async () => {
    sendApi.mockResolvedValue(msg("s1", "rider-1", "x", "04:00"));
    const { result } = setup();
    await settle();
    act(() => result.current.send("🚗".repeat(500)));
    expect(sendApi).toHaveBeenCalledTimes(1);
  });

  it("marks a failed send and retries it with the same Idempotency-Key", async () => {
    sendApi.mockRejectedValueOnce(new TypeError("Failed to fetch"));
    const { result } = setup();
    await settle();
    act(() => result.current.send("hello"));
    await waitFor(() => expect(result.current.messages[0].status).toBe("failed"));
    expect(result.current.notice).toBe("Couldn't send. Tap retry.");
    const failed = result.current.messages[0];

    sendApi.mockResolvedValueOnce(msg("s1", "rider-1", "hello", "04:00"));
    act(() => result.current.retry(failed.id));
    expect(result.current.messages[0].status).toBe("sending");
    await waitFor(() => expect(result.current.messages[0]).toMatchObject({ id: "s1", status: "sent" }));
    expect(result.current.messages).toHaveLength(1);

    expect(sendApi).toHaveBeenCalledTimes(2);
    const keys = sendApi.mock.calls.map((c) => c[2]?.idempotencyKey);
    expect(keys[0]).toBeTruthy();
    expect(keys[1]).toBe(keys[0]);
    expect(sendApi.mock.calls[1][1]).toBe("hello");
  });

  it("uses a new key for a new message", async () => {
    sendApi.mockResolvedValue(msg("s1", "rider-1", "x", "04:00"));
    const { result } = setup();
    await settle();
    act(() => result.current.send("one"));
    act(() => result.current.send("two"));
    const keys = sendApi.mock.calls.map((c) => c[2]?.idempotencyKey);
    expect(new Set(keys).size).toBe(2);
  });

  it("only retries a message that failed", async () => {
    sendApi.mockImplementationOnce(() => new Promise(() => {}));
    const { result } = setup();
    await settle();
    act(() => result.current.send("hello"));
    act(() => result.current.retry(result.current.messages[0].id));
    act(() => result.current.retry("nope"));
    expect(sendApi).toHaveBeenCalledTimes(1);
  });

  it("pauses sending after a rate limit, says for how long, and resumes", async () => {
    vi.useFakeTimers();
    sendApi.mockRejectedValueOnce(new ApiError("slow down", 429, { code: "rate_limited", retryAfter: 3 }));
    const { result } = setup();
    await settle();
    act(() => result.current.send("hello"));
    await settle();

    expect(result.current.messages[0].status).toBe("failed");
    expect(result.current.sendBlocked).toBe(true);
    expect(result.current.notice).toBe("You're sending messages too fast. Try again in 3s.");

    // Nothing goes out while paused, neither a new message nor a retry.
    act(() => result.current.send("again"));
    act(() => result.current.retry(result.current.messages[0].id));
    expect(sendApi).toHaveBeenCalledTimes(1);

    act(() => void vi.advanceTimersByTime(1000));
    expect(result.current.notice).toBe("You're sending messages too fast. Try again in 2s.");
    act(() => void vi.advanceTimersByTime(1000));
    act(() => void vi.advanceTimersByTime(1000));
    expect(result.current.sendBlocked).toBe(false);
    expect(result.current.notice).toBe("");

    sendApi.mockResolvedValueOnce(msg("s1", "rider-1", "hello", "04:00"));
    act(() => result.current.retry(result.current.messages[0].id));
    await settle();
    expect(result.current.messages).toMatchObject([{ id: "s1", status: "sent" }]);
    expect(sendApi.mock.calls[1][2]?.idempotencyKey).toBe(sendApi.mock.calls[0][2]?.idempotencyKey);
  });

  it("turns read-only when the server says the chat is closed", async () => {
    sendApi.mockRejectedValueOnce(new ApiError("closed", 409, { code: "chat_closed" }));
    const { result } = setup();
    await settle();
    act(() => result.current.send("late"));
    await waitFor(() => expect(result.current.closed).toBe(true));
    expect(result.current.messages).toEqual([]);
    expect(result.current.notice).toBe("This chat has ended.");
    act(() => result.current.send("later"));
    expect(sendApi).toHaveBeenCalledTimes(1);
  });

  it("drops a message the server refuses for good, with a reason, instead of offering a retry", async () => {
    sendApi.mockRejectedValueOnce(new ApiError("bad text", 400, { code: "invalid_text" }));
    const { result } = setup();
    await settle();
    act(() => result.current.send("hello"));
    await waitFor(() => expect(result.current.messages).toEqual([]));
    expect(result.current.notice).toMatch(/can't be sent/);
  });

  it("does not touch a new trip when an old send finishes late", async () => {
    let resolve: (m: ChatMessage) => void = () => {};
    sendApi.mockImplementationOnce(() => new Promise((r) => (resolve = r)));
    const { result, rerender } = setup();
    await settle();
    act(() => result.current.send("hello"));
    rerender({ tripId: "trip-2", opts: { me: "rider-1", enabled: true } });
    await settle();
    await act(async () => resolve(msg("s1", "rider-1", "hello", "04:00")));
    expect(result.current.messages).toEqual([]);
  });
});

describe("useChat privacy", () => {
  it("never logs message text", async () => {
    const spies = (["log", "info", "warn", "error", "debug"] as const).map((k) => vi.spyOn(console, k).mockImplementation(() => {}));
    sendApi.mockRejectedValueOnce(new TypeError("Failed to fetch"));
    list.mockResolvedValue(history([hello]));
    const { result } = setup();
    await waitFor(() => expect(result.current.messages).toHaveLength(1));
    act(() => result.current.send("my secret address"));
    await waitFor(() => expect(result.current.messages[1].status).toBe("failed"));
    act(() => void result.current.receive(push(msg("m9", "driver-1", "other secret", "09:00"))));
    for (const spy of spies) expect(JSON.stringify(spy.mock.calls)).not.toMatch(/secret|gate/);
    spies.forEach((s) => s.mockRestore());
  });
});
