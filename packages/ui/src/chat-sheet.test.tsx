import "@testing-library/jest-dom/vitest";
import { render, screen, fireEvent, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ChatButton, ChatSheet, type ChatSheetMessage, type ChatSheetProps } from "./index";

const messages: ChatSheetMessage[] = [
  { id: "m1", mine: false, text: "I am at the gate", createdAt: "2026-01-01T08:00:00Z" },
  { id: "m2", mine: true, text: "Coming now", createdAt: "2026-01-01T08:01:00Z" },
  { id: "m3", mine: true, text: "Still there?", createdAt: "2026-01-01T08:02:00Z", status: "sending" },
  { id: "m4", mine: true, text: "Hello??", createdAt: "2026-01-01T08:03:00Z", status: "failed" },
];

function setup(props: Partial<ChatSheetProps> = {}) {
  const onSend = vi.fn();
  const onRetry = vi.fn();
  const onClose = vi.fn();
  const view = render(
    <ChatSheet open title="Chat with driver-2" messages={messages} onSend={onSend} onRetry={onRetry} onClose={onClose} {...props} />,
  );
  return { onSend, onRetry, onClose, ...view };
}

const field = () => screen.getByRole("textbox", { name: "Message" });
const sendButton = () => screen.getByRole("button", { name: "Send" });

describe("ChatSheet", () => {
  it("renders nothing while closed", () => {
    const { container } = setup({ open: false });
    expect(container).toBeEmptyDOMElement();
  });

  it("is a labelled dialog with a live message log", () => {
    setup();
    expect(screen.getByRole("dialog", { name: "Chat with driver-2" })).toBeInTheDocument();
    const log = screen.getByRole("log", { name: "Messages" });
    expect(log).toHaveAttribute("aria-live", "polite");
    expect(within(log).getAllByRole("listitem")).toHaveLength(4);
  });

  it("puts my messages on the right and theirs on the left, with the time and who said it", () => {
    setup();
    const theirs = screen.getByText("I am at the gate").closest("li")!;
    const mine = screen.getByText("Coming now").closest("li")!;
    expect(theirs).toHaveClass("gm-chat__msg--theirs");
    expect(mine).toHaveClass("gm-chat__msg--mine");
    expect(within(theirs).getByText("Them:")).toBeInTheDocument();
    expect(within(mine).getByText("You:")).toBeInTheDocument();
    const time = mine.querySelector("time")!;
    expect(time).toHaveAttribute("datetime", "2026-01-01T08:01:00Z");
    expect(time.textContent).not.toBe("");
  });

  it("shows sending and failed states, and retries the failed message", () => {
    const { onRetry } = setup();
    expect(screen.getByText("Sending…")).toBeInTheDocument();
    expect(screen.getByText("Still there?").closest(".gm-chat__bubble")).toHaveClass("gm-chat__bubble--pending");
    expect(screen.getByText("Hello??").closest(".gm-chat__bubble")).toHaveClass("gm-chat__bubble--failed");
    fireEvent.click(screen.getByRole("button", { name: "Failed - retry" }));
    expect(onRetry).toHaveBeenCalledWith("m4");
  });

  it("shows a loading state, and an empty state", () => {
    const { rerender } = setup({ messages: [], loading: true });
    expect(screen.getByRole("status")).toHaveTextContent("Loading messages");
    rerender(<ChatSheet open messages={[]} onSend={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByText("No messages yet. Say hello.")).toBeInTheDocument();
  });

  it("keeps showing messages while a reload runs", () => {
    setup({ loading: true });
    expect(screen.queryByText(/Loading messages/)).toBeNull();
    expect(screen.getByText("Coming now")).toBeInTheDocument();
  });

  it("disables send when the field is empty or blank, enables it with text", () => {
    setup();
    expect(sendButton()).toBeDisabled();
    fireEvent.change(field(), { target: { value: "   " } });
    expect(sendButton()).toBeDisabled();
    fireEvent.change(field(), { target: { value: "On my way" } });
    expect(sendButton()).toBeEnabled();
  });

  it("sends the trimmed text with the button and clears the field", () => {
    const { onSend } = setup();
    fireEvent.change(field(), { target: { value: "  On my way " } });
    fireEvent.click(sendButton());
    expect(onSend).toHaveBeenCalledExactlyOnceWith("On my way");
    expect(field()).toHaveValue("");
    expect(sendButton()).toBeDisabled();
  });

  it("sends on Enter, and ignores Shift+Enter", () => {
    const { onSend } = setup();
    fireEvent.change(field(), { target: { value: "one line" } });
    fireEvent.keyDown(field(), { key: "Enter", shiftKey: true });
    expect(onSend).not.toHaveBeenCalled();
    expect(field()).toHaveValue("one line");
    fireEvent.keyDown(field(), { key: "Enter" });
    expect(onSend).toHaveBeenCalledExactlyOnceWith("one line");
  });

  it("never keeps a control character in the field (a paste can carry them)", () => {
    setup();
    fireEvent.change(field(), { target: { value: "a\tb\u0007c" } });
    expect(field()).toHaveValue("a b c");
  });

  it("counts characters and blocks sending over the limit", () => {
    setup({ maxLength: 10 });
    expect(screen.getByText("0/10")).toBeInTheDocument();
    fireEvent.change(field(), { target: { value: "12345" } });
    expect(screen.getByText("5/10")).toBeInTheDocument();
    expect(sendButton()).toBeEnabled();
    fireEvent.change(field(), { target: { value: "12345678901" } });
    expect(screen.getByText("11/10")).toHaveClass("gm-chat__count--over");
    expect(field()).toHaveAttribute("aria-invalid", "true");
    expect(sendButton()).toBeDisabled();
  });

  it("counts an emoji as one character, like the server", () => {
    setup();
    fireEvent.change(field(), { target: { value: "🚗🚗" } });
    expect(screen.getByText("2/500")).toBeInTheDocument();
  });

  it("is read-only with a banner once the chat has ended", () => {
    const { onSend } = setup({ closed: true });
    expect(screen.getByText("This chat has ended")).toBeInTheDocument();
    expect(screen.queryByRole("textbox")).toBeNull();
    expect(screen.queryByRole("button", { name: "Send" })).toBeNull();
    // The history stays readable.
    expect(screen.getByText("I am at the gate")).toBeInTheDocument();
    expect(onSend).not.toHaveBeenCalled();
  });

  it("disables sending while paused, but keeps the field and what was typed", () => {
    const { onSend } = setup({ sendDisabled: true, notice: "Too fast. Try again in 3s." });
    fireEvent.change(field(), { target: { value: "hi" } });
    expect(sendButton()).toBeDisabled();
    fireEvent.keyDown(field(), { key: "Enter" });
    expect(onSend).not.toHaveBeenCalled();
    expect(field()).toHaveValue("hi");
    expect(screen.getByRole("status")).toHaveTextContent("Too fast. Try again in 3s.");
  });

  it("closes with the button and with Escape", () => {
    const { onClose } = setup();
    fireEvent.click(screen.getByRole("button", { name: "Close chat" }));
    fireEvent.keyDown(document, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it("focuses the field on open and scrolls to the newest message", () => {
    const { rerender } = setup();
    expect(field()).toHaveFocus();
    const list = screen.getByRole("log");
    Object.defineProperty(list, "scrollHeight", { value: 480, configurable: true });
    rerender(
      <ChatSheet open messages={[...messages, { id: "m5", mine: false, text: "New one", createdAt: "2026-01-01T08:04:00Z" }]} onSend={vi.fn()} onClose={vi.fn()} />,
    );
    expect(list.scrollTop).toBe(480);
  });
});

describe("ChatButton", () => {
  it("shows an unread badge, capped at 9+, and names the count for screen readers", () => {
    const onClick = vi.fn();
    const { rerender } = render(<ChatButton onClick={onClick} />);
    expect(screen.getByRole("button", { name: "Chat" })).toBeInTheDocument();
    expect(document.querySelector(".gm-chat-button__badge")).toBeNull();

    rerender(<ChatButton unread={1} onClick={onClick} />);
    expect(screen.getByRole("button", { name: "Chat, 1 unread message" })).toHaveTextContent("1");

    rerender(<ChatButton unread={12} onClick={onClick} />);
    const btn = screen.getByRole("button", { name: "Chat, 12 unread messages" });
    expect(btn).toHaveTextContent("9+");
    fireEvent.click(btn);
    expect(onClick).toHaveBeenCalledTimes(1);
  });
});
