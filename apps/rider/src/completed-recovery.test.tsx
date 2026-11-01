import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { Trip } from "@grab-mini/client";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";

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

// What Trip Service holds: rating() records on it, as the real one does.
let stored: Trip;

beforeEach(() => {
  localStorage.clear();
  stored = { id: "trip-1", orderId: "order-1", driverId: "driver-2", status: "COMPLETED", completedAt: new Date().toISOString() };
  vi.mocked(client.listChatMessages).mockResolvedValue({
    messages: [],
    nextBefore: null,
    tripStatus: "COMPLETED",
    open: false,
    closesAt: null,
  });
  vi.spyOn(client, "useNotificationSocket").mockReturnValue("open");
  vi.spyOn(client, "getLatestOrder").mockResolvedValue({
    id: "order-1",
    riderId: "rider-1",
    pickup: { lat: 10.776, lng: 106.7 },
    dropoff: { lat: 10.78, lng: 106.71 },
    vehicleType: "CAR",
    status: "MATCHED",
    price: 100000,
    promoCode: "WELCOME10",
    discount: 10000,
  });
  vi.spyOn(client, "waitForTripByOrder").mockImplementation(async () => ({ ...stored }));
  vi.spyOn(client, "getTripPayments").mockResolvedValue({
    tripId: "trip-1",
    currency: "VND",
    total: 90000,
    payments: [{ id: "p1", kind: "FARE", status: "CAPTURED", amount: 90000, currency: "VND", createdAt: new Date().toISOString() }],
  });
  vi.spyOn(client, "rateTrip").mockImplementation(async (_tripId, _riderId, stars, _c, tip = 0) => {
    stored = { ...stored, rating: stars };
    return { tripId: "trip-1", stars, tipAmount: tip };
  });
  vi.spyOn(client, "getRoute").mockRejectedValue(new Error("no routing"));
  vi.spyOn(client, "getRiderProfile").mockRejectedValue(new Error("no profile service"));
  vi.spyOn(client, "getDriverPublic").mockResolvedValue({
    driverId: "driver-2",
    name: "Minh Tran",
    vehicleType: "CAR",
    model: "Vios",
    color: "White",
    plate: "51A-123.45",
  } as never);
  vi.spyOn(client, "getCancellationPolicy").mockRejectedValue(new Error("no payment"));
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const renderApp = () =>
  render(
    <ToastProvider>
      <App />
    </ToastProvider>,
  );

describe("Rider App after a reload that missed the trip-completed push", () => {
  it("shows the receipt and the rating prompt, and is idle after rating and reloading", async () => {
    renderApp();
    expect(await screen.findByRole("heading", { name: "You've arrived" })).toBeInTheDocument();
    const receipt = await screen.findByLabelText("Receipt");
    expect(receipt).toHaveTextContent(/Fare\s*100\.000đ/);
    expect(receipt).toHaveTextContent(/Promo WELCOME10\s*-10\.000đ/);
    expect(await screen.findByText("How was your ride with Minh Tran?")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("radio", { name: /5/ }));
    fireEvent.click(screen.getByRole("button", { name: "Submit rating" }));
    await waitFor(() => expect(client.rateTrip).toHaveBeenCalledWith("trip-1", "rider-1", 5, "", 0));
    expect(await screen.findByText("Your rating")).toBeInTheDocument();

    // Reload.
    cleanup();
    renderApp();
    expect(await screen.findByRole("heading", { name: "Where to?" })).toBeInTheDocument();
    await waitFor(() => expect(client.waitForTripByOrder).toHaveBeenCalledTimes(2));
    expect(screen.getByRole("heading", { name: "Where to?" })).toBeInTheDocument();
    expect(screen.queryByLabelText("Receipt")).toBeNull();
  });

  it("does not bring the screen back after the rider closed it with New ride", async () => {
    renderApp();
    await screen.findByText("How was your ride with Minh Tran?");
    fireEvent.click(screen.getByRole("button", { name: "New ride" }));
    expect(await screen.findByRole("heading", { name: "Where to?" })).toBeInTheDocument();

    cleanup();
    renderApp();
    await waitFor(() => expect(client.waitForTripByOrder).toHaveBeenCalledTimes(2));
    expect(screen.getByRole("heading", { name: "Where to?" })).toBeInTheDocument();
  });

  it("stays idle, and bookable, when the trip lookup fails", async () => {
    vi.mocked(client.waitForTripByOrder).mockRejectedValue(new Error("trip service down"));
    renderApp();
    await waitFor(() => expect(client.waitForTripByOrder).toHaveBeenCalled());
    expect(screen.getByRole("heading", { name: "Where to?" })).toBeInTheDocument();
  });
});
