import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, renderHook, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { DriverPublic, RiderProfile } from "@grab-mini/client";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";
import { ProfileSheet } from "./ProfileSheet";
import { useDriverCard } from "./useRiderProfile";

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

const profile = (over: Partial<RiderProfile> = {}): RiderProfile => ({
  riderId: "rider-1",
  name: "Nguyen Van An",
  phone: "0901 234 567",
  updatedAt: "2026-01-01T00:00:00Z",
  ...over,
});

const vios: DriverPublic = { driverId: "driver-2", name: "Tran Thi Binh", vehicleType: "CAR", plate: "51A-123.45", model: "Toyota Vios", color: "White" };

beforeEach(() => {
  vi.mocked(client.listChatMessages).mockResolvedValue({ messages: [], nextBefore: null, tripStatus: "ASSIGNED", open: true, closesAt: null });
});
afterEach(() => vi.restoreAllMocks());

describe("ProfileSheet", () => {
  const renderSheet = (over: Partial<Parameters<typeof ProfileSheet>[0]> = {}) => {
    const props = { open: true, riderId: "rider-1", profile: profile(), status: "ready" as const, onRetry: vi.fn(), onSaved: vi.fn(), onClose: vi.fn(), ...over };
    render(<ProfileSheet {...props} />);
    return props;
  };

  it("shows the saved name and phone, saves edits and confirms", async () => {
    const update = vi.spyOn(client, "updateRiderProfile").mockResolvedValue(profile({ name: "An Nguyen" }));
    const props = renderSheet();

    expect(screen.getByLabelText("Name")).toHaveValue("Nguyen Van An");
    expect(screen.getByLabelText("Phone")).toHaveValue("0901 234 567");
    expect(screen.queryByText("Profile saved.")).toBeNull();

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "  An   Nguyen " } });
    fireEvent.click(screen.getByRole("button", { name: "Save profile" }));

    expect(await screen.findByText("Profile saved.")).toBeInTheDocument();
    // Trimmed and collapsed the way the service stores it.
    expect(update).toHaveBeenCalledWith("rider-1", { name: "An Nguyen", phone: "0901 234 567" });
    expect(props.onSaved).toHaveBeenCalledWith(expect.objectContaining({ name: "An Nguyen" }));

    // Editing again clears the confirmation.
    fireEvent.change(screen.getByLabelText("Phone"), { target: { value: "0909" } });
    expect(screen.queryByText("Profile saved.")).toBeNull();
  });

  it("says the phone is not verified, and does not claim otherwise", () => {
    renderSheet();
    expect(screen.getByText(/not checked or verified/)).toBeInTheDocument();
    expect(screen.queryByText(/verified (number|phone)|phone verified/i)).toBeNull();
  });

  it("checks the form before sending and marks each field", () => {
    const update = vi.spyOn(client, "updateRiderProfile");
    renderSheet({ profile: profile({ name: "", phone: "" }) });

    fireEvent.change(screen.getByLabelText("Phone"), { target: { value: "12ab" } });
    fireEvent.click(screen.getByRole("button", { name: "Save profile" }));

    expect(update).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Name")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByText("Enter your name.")).toBeInTheDocument();
    expect(screen.getByLabelText("Phone")).toHaveAttribute("aria-invalid", "true");
    expect(screen.queryByText("Profile saved.")).toBeNull();
  });

  it("puts a refusal from the service next to the field it names", async () => {
    vi.spyOn(client, "updateRiderProfile").mockRejectedValue(
      new client.ApiError("name must contain a letter", 400, { code: "invalid_profile", field: "name" }),
    );
    renderSheet();
    fireEvent.click(screen.getByRole("button", { name: "Save profile" }));

    expect(await screen.findByText("name must contain a letter")).toBeInTheDocument();
    expect(screen.getByLabelText("Name")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Name")).toHaveAccessibleDescription("name must contain a letter");
    expect(screen.queryByText("Profile saved.")).toBeNull();
  });

  it("shows a failure that names no field as a message, and keeps what was typed", async () => {
    vi.spyOn(client, "updateRiderProfile").mockRejectedValue(new client.ApiError("rate limited", 429, { code: "rate_limited" }));
    const props = renderSheet();
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "Binh" } });
    fireEvent.click(screen.getByRole("button", { name: "Save profile" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Too many requests");
    expect(screen.getByLabelText("Name")).toHaveValue("Binh");
    expect(props.onSaved).not.toHaveBeenCalled();
  });

  it("shows a loading state, and offers a retry when the profile could not be loaded", () => {
    const { unmount } = render(
      <ProfileSheet open riderId="rider-1" profile={null} status="loading" onRetry={vi.fn()} onSaved={vi.fn()} onClose={vi.fn()} />,
    );
    expect(screen.getByText("Loading your profile…")).toBeInTheDocument();
    unmount();

    const props = renderSheet({ profile: null, status: "error" });
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(props.onRetry).toHaveBeenCalled();
    // The form is still there: a rider can type a profile even though the stored one couldn't be read.
    expect(screen.getByLabelText("Name")).toHaveValue("");
  });

  it("renders nothing when closed, and Close calls back", () => {
    const { unmount } = render(
      <ProfileSheet open={false} riderId="rider-1" profile={profile()} status="ready" onRetry={vi.fn()} onSaved={vi.fn()} onClose={vi.fn()} />,
    );
    expect(screen.queryByRole("dialog")).toBeNull();
    unmount();
    const props = renderSheet();
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: /^Close/ }));
    expect(props.onClose).toHaveBeenCalled();
  });
});

describe("Rider App", () => {
  beforeEach(() => {
    vi.spyOn(client, "useNotificationSocket").mockReturnValue("open");
    vi.spyOn(client, "getLatestOrder").mockResolvedValue(null);
    vi.spyOn(client, "getRoute").mockRejectedValue(new Error("no routing"));
    vi.spyOn(client, "getQuote").mockRejectedValue(new Error("no pricing"));
  });

  const renderApp = () =>
    render(
      <ToastProvider>
        <App />
      </ToastProvider>,
    );

  it("takes the avatar initials from the profile name and opens the profile from the avatar", async () => {
    vi.spyOn(client, "getRiderProfile").mockResolvedValue(profile());
    renderApp();

    const avatar = screen.getByRole("button", { name: "Your profile" });
    await waitFor(() => expect(within(avatar).getByRole("img")).toHaveAttribute("aria-label", "Nguyen Van An"));
    expect(avatar).toHaveTextContent("NA");

    fireEvent.click(avatar);
    expect(await screen.findByRole("dialog", { name: "Your profile" })).toBeInTheDocument();
    expect(screen.getByLabelText("Name")).toHaveValue("Nguyen Van An");
  });

  it("falls back to the id for the avatar when the profile can't be read, and still opens the sheet", async () => {
    const get = vi.spyOn(client, "getRiderProfile").mockRejectedValue(new Error("profile service down"));
    renderApp();
    await waitFor(() => expect(get).toHaveBeenCalledWith("rider-1"));

    const avatar = screen.getByRole("button", { name: "Your profile" });
    expect(avatar).toHaveTextContent("R1");
    fireEvent.click(avatar);
    expect(await screen.findByRole("button", { name: "Try again" })).toBeInTheDocument();
  });

  it("updates the avatar after a save", async () => {
    vi.spyOn(client, "getRiderProfile").mockResolvedValue(profile({ name: "", phone: "", updatedAt: null }));
    vi.spyOn(client, "updateRiderProfile").mockResolvedValue(profile({ name: "Le Minh", updatedAt: "2026-02-01T00:00:00Z" }));
    renderApp();

    fireEvent.click(screen.getByRole("button", { name: "Your profile" }));
    fireEvent.change(await screen.findByLabelText("Name"), { target: { value: "Le Minh" } });
    fireEvent.click(screen.getByRole("button", { name: "Save profile" }));

    expect(await screen.findByText("Profile saved.")).toBeInTheDocument();
    expect(screen.getAllByRole("img", { name: "Le Minh" }).length).toBeGreaterThan(0);
  });
});

describe("driver card on a matched ride", () => {
  beforeEach(() => {
    vi.spyOn(client, "useNotificationSocket").mockReturnValue("open");
    vi.spyOn(client, "getRiderProfile").mockRejectedValue(new Error("no profile service"));
    vi.spyOn(client, "getRoute").mockRejectedValue(new Error("no routing"));
    vi.spyOn(client, "getQuote").mockRejectedValue(new Error("no pricing"));
    vi.spyOn(client, "getCancellationPolicy").mockRejectedValue(new Error("no payment"));
    vi.spyOn(client, "getLatestOrder").mockResolvedValue({
      id: "order-1",
      riderId: "rider-1",
      pickup: { lat: 10.776, lng: 106.7 },
      dropoff: { lat: 10.78, lng: 106.71 },
      vehicleType: "CAR",
      status: "MATCHED",
    });
    vi.spyOn(client, "waitForTripByOrder").mockResolvedValue({ id: "trip-1", orderId: "order-1", driverId: "driver-2", status: "ASSIGNED" });
  });

  const renderApp = () =>
    render(
      <ToastProvider>
        <App />
      </ToastProvider>,
    );

  it("shows the driver's name, vehicle and plate, asking once", async () => {
    const lookup = vi.spyOn(client, "getDriverPublic").mockResolvedValue(vios);
    renderApp();

    expect(await screen.findByText("Tran Thi Binh")).toBeInTheDocument();
    expect(screen.getByText("Toyota Vios · White · Car")).toBeInTheDocument();
    expect(screen.getByLabelText("Plate number")).toHaveTextContent("51A-123.45");
    // The id is no longer the headline.
    expect(screen.queryByText("driver-2")).toBeNull();
    expect(lookup).toHaveBeenCalledTimes(1);
    expect(lookup).toHaveBeenCalledWith("driver-2");
  });

  it("shows the id as before when there is no approved profile", async () => {
    const lookup = vi.spyOn(client, "getDriverPublic").mockResolvedValue(null);
    renderApp();

    expect(await screen.findByText("driver-2")).toBeInTheDocument();
    expect(screen.getByText("Your driver · Car")).toBeInTheDocument();
    await waitFor(() => expect(lookup).toHaveBeenCalled());
    expect(screen.queryByLabelText("Plate number")).toBeNull();
  });

  it("shows the id when the lookup fails (rate limited), and the ride carries on", async () => {
    vi.spyOn(client, "getDriverPublic").mockRejectedValue(new client.ApiError("slow down", 429, { code: "rate_limited" }));
    renderApp();

    expect(await screen.findByText("driver-2")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Your driver is on the way" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Cancel trip" })).toBeInTheDocument();
  });

  it("does not wait for the lookup", async () => {
    vi.spyOn(client, "getDriverPublic").mockReturnValue(new Promise(() => {}));
    renderApp();

    expect(await screen.findByText("driver-2")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Your driver is on the way" })).toBeInTheDocument();
  });
});

describe("useDriverCard", () => {
  it("asks once per driver, ignores a stale answer and never shows one driver's card for another", async () => {
    let resolveFirst: (d: DriverPublic | null) => void = () => {};
    const lookup = vi
      .spyOn(client, "getDriverPublic")
      .mockImplementationOnce(() => new Promise((r) => (resolveFirst = r)))
      .mockResolvedValueOnce({ ...vios, driverId: "driver-3", name: "Pham Van Cuong" });
    const { result, rerender } = renderHook(({ id }: { id: string | null }) => useDriverCard(id), { initialProps: { id: "driver-2" as string | null } });

    rerender({ id: "driver-2" });
    expect(lookup).toHaveBeenCalledTimes(1);

    rerender({ id: "driver-3" });
    await waitFor(() => expect(result.current?.name).toBe("Pham Van Cuong"));
    expect(lookup).toHaveBeenCalledTimes(2);

    // driver-2's slow answer arrives after the ride moved on.
    await act(async () => resolveFirst(vios));
    expect(result.current?.name).toBe("Pham Van Cuong");

    rerender({ id: null });
    expect(result.current).toBeNull();
    expect(lookup).toHaveBeenCalledTimes(2);
  });
});
