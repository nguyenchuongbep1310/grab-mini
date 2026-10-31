import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, renderHook, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { ChatHistory, ChatMessage } from "@grab-mini/client";
import type { NotificationMessage } from "@grab-mini/events";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";
import { initialRide, useRide, type RideState } from "./useRide";

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
  senderRole: senderId === "rider-1" ? "rider" : "driver",
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
      recipientId: "rider-1",
      recipientRole: "rider",
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
  vi.spyOn(client, "getLatestOrder").mockResolvedValue({
    id: "order-1",
    riderId: "rider-1",
    pickup: { lat: 10.776, lng: 106.7 },
    dropoff: { lat: 10.78, lng: 106.71 },
    vehicleType: "CAR",
    status: "MATCHED",
  });
  vi.spyOn(client, "waitForTripByOrder").mockResolvedValue({ id: "trip-1", orderId: "order-1", driverId: "driver-2", status: "ASSIGNED" });
  vi.spyOn(client, "getRoute").mockRejectedValue(new Error("no routing"));
  // The profile and the driver card only decorate; these tests are about something else.
  vi.spyOn(client, "getRiderProfile").mockRejectedValue(new Error("no profile service"));
  vi.spyOn(client, "getDriverPublic").mockResolvedValue(null);
  vi.spyOn(client, "getCancellationPolicy").mockRejectedValue(new Error("no payment"));
});
afterEach(() => vi.restoreAllMocks());

const renderApp = () =>
  render(
    <ToastProvider>
      <App />
    </ToastProvider>,
  );

describe("Rider App chat", () => {
  it("shows a Chat button with an unread badge, opens the sheet, shows and sends messages", async () => {
    list.mockResolvedValue(history([chatMessage("m0", "driver-2", "Earlier note", "00:00")]));
    renderApp();

    const button = await screen.findByRole("button", { name: "Chat" });
    await waitFor(() => expect(list).toHaveBeenCalledWith("trip-1", { limit: 100 }));
    expect(screen.queryByRole("dialog")).toBeNull();

    // A push while the sheet is closed raises the badge, and does not disturb the ride.
    act(() => deliver(chatPush(chatMessage("m1", "driver-2", "I am at the gate", "01:00"))));
    expect(await screen.findByRole("button", { name: "Chat, 1 unread message" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Your driver is on the way" })).toBeInTheDocument();
    expect(button).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /^Chat/ }));
    const dialog = screen.getByRole("dialog", { name: "Chat with driver-2" });
    expect(within(dialog).getByText("Earlier note")).toBeInTheDocument();
    expect(within(dialog).getByText("I am at the gate")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Chat" })).toBeInTheDocument(); // badge cleared

    // A push while it is open appends and is not counted.
    act(() => deliver(chatPush(chatMessage("m2", "driver-2", "Take your time", "02:00"))));
    expect(within(dialog).getByText("Take your time")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Chat" })).toBeInTheDocument();

    sendApi.mockResolvedValue(chatMessage("m3", "rider-1", "Coming down", "03:00"));
    const field = within(dialog).getByRole("textbox", { name: "Message" });
    fireEvent.change(field, { target: { value: "Coming down" } });
    fireEvent.keyDown(field, { key: "Enter" });
    expect(sendApi).toHaveBeenCalledWith("trip-1", "Coming down", { idempotencyKey: expect.any(String) });
    await waitFor(() => expect(within(dialog).queryByText("Sending…")).toBeNull());
    expect(within(dialog).getByText("Coming down")).toBeInTheDocument();

    fireEvent.click(within(dialog).getByRole("button", { name: "Close chat" }));
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("has no Chat button while searching", async () => {
    vi.spyOn(client, "getLatestOrder").mockResolvedValue({
      id: "order-1",
      riderId: "rider-1",
      pickup: { lat: 10.776, lng: 106.7 },
      dropoff: { lat: 10.78, lng: 106.71 },
      vehicleType: "CAR",
      status: "PENDING",
    });
    renderApp();
    await screen.findByRole("heading", { name: "Looking for a driver" });
    expect(screen.queryByRole("button", { name: /^Chat/ })).toBeNull();
    expect(list).not.toHaveBeenCalled();
  });

  it("keeps the chat readable after the trip is over", async () => {
    list.mockResolvedValue(history([chatMessage("m0", "driver-2", "Thanks!", "00:00")]));
    renderApp();
    await screen.findByRole("button", { name: "Chat" });
    await waitFor(() => expect(list).toHaveBeenCalled());

    act(() =>
      deliver({
        type: "trip.status.changed",
        payload: {
          eventId: "e-9",
          occurredAt: "2026-01-01T09:00:00Z",
          tripId: "trip-1",
          orderId: "order-1",
          riderId: "rider-1",
          driverId: "driver-2",
          status: "COMPLETED",
          cancelledBy: "",
          reason: "",
        },
      } as NotificationMessage),
    );
    await screen.findByRole("heading", { name: "You've arrived" });
    fireEvent.click(screen.getByRole("button", { name: "Chat" }));
    expect(screen.getByText("Thanks!")).toBeInTheDocument();
    // Within the grace period the server still says it is open.
    expect(screen.getByRole("textbox", { name: "Message" })).toBeInTheDocument();
  });

  it("shows the read-only banner when the server has closed the chat", async () => {
    list.mockResolvedValue(history([chatMessage("m0", "driver-2", "Thanks!", "00:00")], { open: false, tripStatus: "COMPLETED" }));
    renderApp();
    fireEvent.click(await screen.findByRole("button", { name: "Chat" }));
    expect(await screen.findByText("This chat has ended")).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Message" })).toBeNull();
  });
});

describe("useRide with pushes it has no case for", () => {
  const unknownPush = (type: string, payload: object = { tripId: "trip-1", orderId: "nope", text: "hi" }) =>
    ({ type, payload }) as unknown as NotificationMessage;
  const completed = {
    type: "trip.status.changed",
    payload: { eventId: "e", occurredAt: "t", tripId: "trip-1", orderId: "order-1", riderId: "rider-1", driverId: "driver-2", status: "COMPLETED", cancelledBy: "", reason: "" },
  } as NotificationMessage;
  const restored = (phase: "searching" | "assigned" | "in_progress") =>
    ({ type: "restored", ride: { orderId: "order-1", phase, vehicleType: "CAR", dropoff: { lat: 1, lng: 2 }, driverId: "driver-2", tripId: "trip-1" } }) as const;

  const reaching: Record<string, (dispatch: ReturnType<typeof useRide>[1]) => void> = {
    idle: () => {},
    booking: (d) => d({ type: "bookStarted" }),
    searching: (d) => d(restored("searching")),
    assigned: (d) => d(restored("assigned")),
    in_progress: (d) => d(restored("in_progress")),
    completed: (d) => {
      d(restored("in_progress"));
      d({ type: "notification", message: completed });
    },
    cancelled: (d) => {
      d(restored("searching"));
      d({ type: "orderCancelled", orderId: "order-1" });
    },
  };

  it.each(Object.keys(reaching))("a chat push or an unknown push does not change the ride or crash it (%s)", (phase) => {
    const { result } = renderHook(() => useRide());
    act(() => reaching[phase](result.current[1]));
    expect(result.current[0].phase).toBe(phase);
    const before = result.current[0];
    for (const message of [unknownPush("chat.message.sent"), unknownPush("chat.message.sent", {}), unknownPush("chat.message.sent", { orderId: "order-1" }), unknownPush("something.new")]) {
      expect(() => act(() => result.current[1]({ type: "notification", message }))).not.toThrow();
      expect(result.current[0]).toBe(before);
    }
  });
});
