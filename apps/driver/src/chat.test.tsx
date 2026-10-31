import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, renderHook, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { ChatHistory, ChatMessage } from "@grab-mini/client";
import type { NotificationMessage } from "@grab-mini/events";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";
import { useDriverTrip } from "./useDriverTrip";

// The chat hook calls the api module directly, so that is where the network is replaced.
vi.mock("../../../packages/client/src/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../packages/client/src/api")>()),
  listChatMessages: vi.fn(),
  sendChatMessage: vi.fn(),
}));

// Leaflet needs a real browser.
vi.mock("@grab-mini/ui", async (importOriginal) => {
  const ui = await importOriginal<typeof import("@grab-mini/ui")>();
  return { ...ui, MapView: () => null, MapMarker: () => null };
});

const list = vi.mocked(client.listChatMessages);
const sendApi = vi.mocked(client.sendChatMessage);

const chatMessage = (id: string, senderId: string, text: string, at: string): ChatMessage => ({
  id,
  tripId: "trip-1",
  senderId,
  senderRole: senderId === "driver-1" ? "driver" : "rider",
  text,
  createdAt: `2026-01-01T08:${at}Z`,
});

const chatPush = (m: ChatMessage): NotificationMessage =>
  ({
    type: "chat.message.sent",
    payload: {
      eventId: `e-${m.id}`,
      occurredAt: m.createdAt,
      messageId: m.id,
      tripId: m.tripId,
      senderId: m.senderId,
      senderRole: m.senderRole,
      recipientId: "driver-1",
      recipientRole: "driver",
      text: m.text,
    },
  }) as unknown as NotificationMessage;

const history = (messages: ChatMessage[], extra: Partial<ChatHistory> = {}): ChatHistory => ({
  messages: [...messages].reverse(),
  nextBefore: null,
  tripStatus: "ASSIGNED",
  open: true,
  closesAt: null,
  ...extra,
});

let deliver: (m: NotificationMessage) => void = () => {};

beforeEach(() => {
  list.mockReset();
  sendApi.mockReset();
  list.mockResolvedValue(history([]));
  vi.spyOn(client, "useNotificationSocket").mockImplementation((_role, _id, onMessage) => {
    deliver = onMessage;
    return "open";
  });
  vi.spyOn(client, "getActiveTrip").mockResolvedValue({ id: "trip-1", orderId: "order-1", driverId: "driver-1", status: "ASSIGNED" });
  vi.spyOn(client, "waitForOrder").mockResolvedValue({
    id: "order-1",
    riderId: "rider-7",
    pickup: { lat: 10.776, lng: 106.7 },
    dropoff: { lat: 10.78, lng: 106.71 },
    vehicleType: "CAR",
    status: "MATCHED",
  });
  // An approved driver: the normal app.
  vi.spyOn(client, "getDriverProfile").mockResolvedValue({
    driverId: "driver-1", name: "Driver One", phone: "", vehicleType: "CAR", plate: "51A-123.45", model: "", color: "",
    status: "APPROVED", reason: "", reviewedAt: null, statusAt: null, updatedAt: null,
  });
  vi.spyOn(client, "getDriverRating").mockRejectedValue(new Error("no rating"));
  vi.spyOn(client, "getDriverEarnings").mockRejectedValue(new Error("no payments"));
});
afterEach(() => vi.restoreAllMocks());

const renderApp = () =>
  render(
    <ToastProvider>
      <App />
    </ToastProvider>,
  );

describe("Driver App chat", () => {
  it("shows a Chat button with an unread badge, opens the sheet, shows and sends messages", async () => {
    renderApp();
    await screen.findByRole("button", { name: "Chat" });
    await waitFor(() => expect(list).toHaveBeenCalledWith("trip-1", { limit: 100 }));

    // A push while the sheet is closed raises the badge, and does not disturb the trip.
    act(() => deliver(chatPush(chatMessage("m1", "rider-7", "I'm by the blue door", "01:00"))));
    act(() => deliver(chatPush(chatMessage("m2", "rider-7", "Wearing a red jacket", "01:30"))));
    expect(await screen.findByRole("button", { name: "Chat, 2 unread messages" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "New trip" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /^Chat/ }));
    const dialog = screen.getByRole("dialog", { name: "Chat with rider-7" });
    expect(within(dialog).getByText("I'm by the blue door")).toBeInTheDocument();
    expect(within(dialog).getByText("Wearing a red jacket")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Chat" })).toBeInTheDocument(); // badge cleared

    sendApi.mockResolvedValue(chatMessage("m3", "driver-1", "Two minutes away", "02:00"));
    const field = within(dialog).getByRole("textbox", { name: "Message" });
    fireEvent.change(field, { target: { value: "Two minutes away" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Send" }));
    expect(sendApi).toHaveBeenCalledWith("trip-1", "Two minutes away", { idempotencyKey: expect.any(String) });
    await waitFor(() => expect(within(dialog).queryByText("Sending…")).toBeNull());
    expect(within(dialog).getByText("Two minutes away")).toBeInTheDocument();
  });

  it("offers a retry for a message that failed", async () => {
    renderApp();
    fireEvent.click(await screen.findByRole("button", { name: "Chat" }));
    sendApi.mockRejectedValueOnce(new TypeError("Failed to fetch"));
    fireEvent.change(screen.getByRole("textbox", { name: "Message" }), { target: { value: "hello" } });
    fireEvent.keyDown(screen.getByRole("textbox", { name: "Message" }), { key: "Enter" });

    sendApi.mockResolvedValueOnce(chatMessage("m1", "driver-1", "hello", "03:00"));
    fireEvent.click(await screen.findByRole("button", { name: "Failed - retry" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Failed - retry" })).toBeNull());
    const keys = sendApi.mock.calls.map((c) => c[2]?.idempotencyKey);
    expect(keys).toHaveLength(2);
    expect(keys[1]).toBe(keys[0]);
  });

  it("has no Chat button when there is no trip", async () => {
    vi.spyOn(client, "getActiveTrip").mockResolvedValue(null);
    renderApp();
    await screen.findByRole("heading", { name: "You're online" });
    expect(screen.queryByRole("button", { name: /^Chat/ })).toBeNull();
    expect(list).not.toHaveBeenCalled();
    // A chat push with no trip is harmless.
    expect(() => act(() => deliver(chatPush(chatMessage("m1", "rider-7", "hi", "01:00"))))).not.toThrow();
    expect(screen.getByRole("heading", { name: "You're online" })).toBeInTheDocument();
  });
});

describe("useDriverTrip with pushes it has no case for", () => {
  const push = (type: string, payload: object) => ({ type, payload }) as unknown as NotificationMessage;
  const matched = {
    type: "order.matched",
    payload: { eventId: "e", orderId: "order-1", driverId: "driver-1", riderId: "rider-7", vehicleType: "CAR", occurredAt: "t" },
  } as NotificationMessage;

  const reaching = {
    idle: () => {},
    assigned: (d: ReturnType<typeof useDriverTrip>[1]) => d({ type: "notification", driverId: "driver-1", message: matched }),
    in_progress: (d: ReturnType<typeof useDriverTrip>[1]) => d({ type: "restored", tripId: "trip-1", orderId: "order-1", status: "IN_PROGRESS" }),
  };

  it.each(Object.keys(reaching) as (keyof typeof reaching)[])("a chat push or an unknown push does not change the trip or crash it (%s)", (phase) => {
    const { result } = renderHook(() => useDriverTrip());
    act(() => reaching[phase](result.current[1]));
    expect(result.current[0].phase).toBe(phase);
    const before = result.current[0];
    const payloads = [{}, { tripId: "trip-1", orderId: "order-1", driverId: "driver-1", recipientId: "driver-1", text: "hi" }];
    for (const type of ["chat.message.sent", "something.new"]) {
      for (const payload of payloads) {
        expect(() => act(() => result.current[1]({ type: "notification", driverId: "driver-1", message: push(type, payload) }))).not.toThrow();
        expect(result.current[0]).toBe(before);
      }
    }
  });
});
