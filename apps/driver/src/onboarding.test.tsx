import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, renderHook, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { DriverProfile, DriverStatus } from "@grab-mini/client";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";
import { DriverForm } from "./DriverForm";
import { classifyFailure, gateOf, type ProfileLoad } from "./onboarding";
import { PENDING_POLL_MS, useDriverProfile } from "./useDriverProfile";

// Leaflet needs a real browser.
vi.mock("@grab-mini/ui", async (importOriginal) => {
  const ui = await importOriginal<typeof import("@grab-mini/ui")>();
  return { ...ui, MapView: () => null, MapMarker: () => null };
});

const profile = (status: DriverStatus, over: Partial<DriverProfile> = {}): DriverProfile => ({
  driverId: "driver-9",
  name: status === "NONE" ? "" : "Pham Van Cuong",
  phone: "",
  vehicleType: status === "NONE" ? "" : "CAR",
  plate: status === "NONE" ? "" : "59-X1 123.45",
  model: status === "NONE" ? "" : "Honda City",
  color: status === "NONE" ? "" : "Black",
  status,
  reason: "",
  reviewedAt: null,
  statusAt: null,
  updatedAt: null,
  ...over,
});

const apiError = (status: number, code?: string, body: Record<string, unknown> = {}) =>
  new client.ApiError(`failed ${status}`, status, code ? { code, ...body } : body);

afterEach(() => vi.restoreAllMocks());

describe("gateOf: the five statuses and the ways loading can end", () => {
  const gate = (load: ProfileLoad) => gateOf(load).kind;
  const loaded = (status: DriverStatus): ProfileLoad => ({ kind: "loaded", profile: profile(status) });

  it("maps each status to its screen; only APPROVED reaches the app", () => {
    expect(gate(loaded("NONE"))).toBe("onboarding");
    expect(gate(loaded("PENDING"))).toBe("pending");
    expect(gate(loaded("REJECTED"))).toBe("rejected");
    expect(gate(loaded("SUSPENDED"))).toBe("suspended");
    expect(gate(loaded("APPROVED"))).toBe("app");
    expect(gateOf(loaded("APPROVED"))).toMatchObject({ profile: { status: "APPROVED" } });
  });

  it("never lets a status it doesn't know through", () => {
    expect(gate({ kind: "loaded", profile: profile("APPROVED", { status: "ARCHIVED" as DriverStatus }) })).toBe("denied");
  });

  it("checks while loading; an outage falls back to the app with no profile, a refusal does not", () => {
    expect(gate({ kind: "loading" })).toBe("checking");
    expect(gateOf({ kind: "unavailable" })).toEqual({ kind: "app", profile: null });
    expect(gate({ kind: "denied" })).toBe("denied");
  });

  it("classifies failures: only ones that say nothing about this driver are 'unavailable'", () => {
    for (const status of [500, 502, 503, 504, 429, 408]) expect(classifyFailure(apiError(status))).toBe("unavailable");
    expect(classifyFailure(new TypeError("Failed to fetch"))).toBe("unavailable");
    for (const status of [400, 401, 403, 404]) expect(classifyFailure(apiError(status, status === 403 ? "forbidden" : undefined))).toBe("denied");
  });
});

describe("useDriverProfile", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const flush = () => act(async () => {});

  it("re-checks a pending driver on a timer and stops once they are approved", async () => {
    const get = vi
      .spyOn(client, "getDriverProfile")
      .mockResolvedValueOnce(profile("PENDING"))
      .mockResolvedValueOnce(profile("PENDING"))
      .mockResolvedValue(profile("APPROVED"));
    const { result } = renderHook(() => useDriverProfile("driver-9"));
    expect(result.current.gate.kind).toBe("checking");
    await flush();
    expect(result.current.gate.kind).toBe("pending");
    expect(get).toHaveBeenCalledTimes(1);

    await act(async () => void (await vi.advanceTimersByTimeAsync(PENDING_POLL_MS)));
    expect(get).toHaveBeenCalledTimes(2);
    expect(result.current.gate.kind).toBe("pending");

    await act(async () => void (await vi.advanceTimersByTimeAsync(PENDING_POLL_MS)));
    expect(result.current.gate.kind).toBe("app");
    expect(get).toHaveBeenCalledTimes(3);

    // Approved: no more polling.
    await act(async () => void (await vi.advanceTimersByTimeAsync(PENDING_POLL_MS * 5)));
    expect(get).toHaveBeenCalledTimes(3);
  });

  it("does not poll for the other statuses", async () => {
    const get = vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("REJECTED", { reason: "Blurry plate" }));
    renderHook(() => useDriverProfile("driver-9"));
    await flush();
    await act(async () => void (await vi.advanceTimersByTimeAsync(PENDING_POLL_MS * 3)));
    expect(get).toHaveBeenCalledTimes(1);
  });

  it("keeps what it knew when a later check fails", async () => {
    const get = vi.spyOn(client, "getDriverProfile").mockResolvedValueOnce(profile("PENDING")).mockRejectedValue(apiError(503));
    const { result } = renderHook(() => useDriverProfile("driver-9"));
    await flush();
    await act(async () => void (await vi.advanceTimersByTimeAsync(PENDING_POLL_MS)));
    expect(get).toHaveBeenCalledTimes(2);
    // Still waiting for review: an outage must not turn into "unknown, show the app".
    expect(result.current.gate.kind).toBe("pending");
  });

  it("ends the first load as 'app, unknown' on an outage and as 'denied' on a 403 or 404", async () => {
    const get = vi.spyOn(client, "getDriverProfile").mockRejectedValueOnce(apiError(503));
    const { result } = renderHook(() => useDriverProfile("driver-9"));
    await flush();
    expect(result.current.gate).toEqual({ kind: "app", profile: null });

    get.mockRejectedValueOnce(apiError(403, "forbidden"));
    await act(async () => void (await result.current.retry()));
    expect(result.current.gate.kind).toBe("denied");

    get.mockRejectedValueOnce(apiError(404, "not_found"));
    await act(async () => void (await result.current.retry()));
    expect(result.current.gate.kind).toBe("denied");

    get.mockResolvedValueOnce(profile("APPROVED"));
    await act(async () => void (await result.current.retry()));
    expect(result.current.gate.kind).toBe("app");
  });
});

describe("DriverForm", () => {
  const fill = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });
  const fillAll = () => {
    fill("Name", "  Pham   Van Cuong ");
    fill("Vehicle type", "CAR");
    fill("Plate number", "59-x1 123.45");
    fill("Vehicle model", "Honda City");
    fill("Vehicle colour", "Black");
  };

  it("does not send an incomplete form and marks each missing field", () => {
    const update = vi.spyOn(client, "updateDriverProfile");
    render(<DriverForm driverId="driver-9" initial={profile("NONE")} mode="apply" submitLabel="Submit application" onDone={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Submit application" }));

    expect(update).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Name")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Vehicle type")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Plate number")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Vehicle model")).not.toHaveAttribute("aria-invalid");
  });

  it("saves the profile, then applies, and passes on the profile that apply returned", async () => {
    const order: string[] = [];
    const update = vi.spyOn(client, "updateDriverProfile").mockImplementation(async () => (order.push("update"), profile("NONE")));
    const apply = vi.spyOn(client, "applyAsDriver").mockImplementation(async () => (order.push("apply"), profile("PENDING")));
    const onDone = vi.fn();
    render(<DriverForm driverId="driver-9" initial={profile("NONE")} mode="apply" submitLabel="Submit application" onDone={onDone} />);
    fillAll();
    fireEvent.click(screen.getByRole("button", { name: "Submit application" }));

    await waitFor(() => expect(onDone).toHaveBeenCalledWith(expect.objectContaining({ status: "PENDING" })));
    expect(order).toEqual(["update", "apply"]);
    expect(update).toHaveBeenCalledWith("driver-9", { name: "Pham Van Cuong", phone: "", vehicleType: "CAR", plate: "59-X1 123.45", model: "Honda City", color: "Black" });
    expect(apply).toHaveBeenCalledWith("driver-9");
  });

  it("does not apply when saving is refused, and shows the refusal at its field", async () => {
    vi.spyOn(client, "updateDriverProfile").mockRejectedValue(apiError(400, "invalid_profile", { field: "plate" }));
    const apply = vi.spyOn(client, "applyAsDriver");
    render(<DriverForm driverId="driver-9" initial={profile("NONE")} mode="apply" submitLabel="Submit application" onDone={vi.fn()} />);
    fillAll();
    fireEvent.click(screen.getByRole("button", { name: "Submit application" }));

    expect(await screen.findByText("failed 400")).toBeInTheDocument();
    expect(screen.getByLabelText("Plate number")).toHaveAttribute("aria-invalid", "true");
    expect(apply).not.toHaveBeenCalled();
  });

  it("keeps the form, and can resubmit, when applying fails after the details were saved", async () => {
    const update = vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("NONE"));
    const apply = vi.spyOn(client, "applyAsDriver").mockRejectedValueOnce(apiError(503)).mockResolvedValueOnce(profile("PENDING"));
    const onDone = vi.fn();
    render(<DriverForm driverId="driver-9" initial={profile("NONE")} mode="apply" submitLabel="Submit application" onDone={onDone} />);
    fillAll();
    fireEvent.click(screen.getByRole("button", { name: "Submit application" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("failed 503");
    expect(onDone).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Plate number")).toHaveValue("59-x1 123.45");

    fireEvent.click(screen.getByRole("button", { name: "Submit application" }));
    await waitFor(() => expect(onDone).toHaveBeenCalled());
    expect(update).toHaveBeenCalledTimes(2);
    expect(apply).toHaveBeenCalledTimes(2);
  });

  it("says a suspended driver can't apply when the service refuses (409 suspended)", async () => {
    vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("SUSPENDED"));
    vi.spyOn(client, "applyAsDriver").mockRejectedValue(apiError(409, "suspended"));
    render(<DriverForm driverId="driver-9" initial={profile("REJECTED")} mode="apply" submitLabel="Fix and re-apply" onDone={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Fix and re-apply" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("suspended");
  });

  describe("edit (approved driver)", () => {
    const renderEdit = (onDone = vi.fn()) => {
      render(<DriverForm driverId="driver-9" initial={profile("APPROVED")} mode="edit" submitLabel="Save changes" onDone={onDone} />);
      return onDone;
    };

    it("always warns that a new plate or vehicle type sends the profile back to review", () => {
      renderEdit();
      expect(screen.getByRole("note")).toHaveTextContent(/plate number or the vehicle type sends your profile back to review/);
      expect(screen.getByRole("note")).toHaveTextContent(/not be offered trips/);
    });

    it("saves a new model and colour at once, with no applying", async () => {
      const update = vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("APPROVED", { color: "Red" }));
      const apply = vi.spyOn(client, "applyAsDriver");
      const onDone = renderEdit();
      fill("Vehicle colour", "Red");
      fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

      await waitFor(() => expect(onDone).toHaveBeenCalledWith(expect.objectContaining({ status: "APPROVED" })));
      expect(update).toHaveBeenCalledWith("driver-9", expect.objectContaining({ color: "Red" }));
      expect(apply).not.toHaveBeenCalled();
    });

    it("asks for a second, explicit click when the plate changes, and can back out", async () => {
      const update = vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("PENDING", { plate: "51A-123.45" }));
      const onDone = renderEdit();
      fill("Plate number", "51a-123.45");
      fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

      expect(update).not.toHaveBeenCalled();
      expect(screen.getByRole("alert")).toHaveTextContent(/takes you offline until an admin approves/);
      fireEvent.click(screen.getByRole("button", { name: "Keep editing" }));
      expect(screen.queryByRole("alert")).toBeNull();

      fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
      fireEvent.click(screen.getByRole("button", { name: "Save and send for review" }));
      await waitFor(() => expect(onDone).toHaveBeenCalledWith(expect.objectContaining({ status: "PENDING" })));
      expect(update).toHaveBeenCalledWith("driver-9", expect.objectContaining({ plate: "51A-123.45" }));
    });

    it("treats a plate typed in other case or spacing as unchanged, and a new vehicle type as a change", async () => {
      const update = vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("APPROVED"));
      renderEdit();
      fill("Plate number", "  59-x1   123.45 ");
      fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
      expect(update).toHaveBeenCalledTimes(1);
      expect(screen.queryByRole("alert")).toBeNull();
      await waitFor(() => expect(screen.getByRole("button", { name: "Save changes" })).toBeEnabled());

      fill("Vehicle type", "BIKE");
      fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
      expect(screen.getByRole("alert")).toBeInTheDocument();
      expect(update).toHaveBeenCalledTimes(1);
    });
  });
});

describe("Driver App onboarding", () => {
  beforeEach(() => {
    vi.spyOn(client, "useNotificationSocket").mockReturnValue("open");
    vi.spyOn(client, "getActiveTrip").mockResolvedValue(null);
    vi.spyOn(client, "getDriverRating").mockRejectedValue(new Error("no rating"));
    vi.spyOn(client, "getDriverEarnings").mockRejectedValue(new Error("no payments"));
  });

  const renderApp = () =>
    render(
      <ToastProvider>
        <App />
      </ToastProvider>,
    );
  const noTrips = () => {
    expect(screen.queryByText("You're online")).toBeNull();
    expect(screen.queryByText("Waiting for the next trip request.")).toBeNull();
    expect(screen.queryByText("AVAILABLE")).toBeNull();
    expect(screen.queryByText("Available")).toBeNull();
  };

  it("NONE: shows the application form and no trips", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("NONE"));
    renderApp();
    expect(await screen.findByRole("heading", { name: "Become a driver" })).toBeInTheDocument();
    expect(screen.getByRole("form", { name: "Driver application" })).toBeInTheDocument();
    noTrips();
    // Not "Online": the header chip only says the connection is up.
    expect(screen.getByText("Connected")).toBeInTheDocument();
  });

  it("PENDING: shows 'Waiting for review' with what was submitted, no form, no trips", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("PENDING"));
    renderApp();
    expect(await screen.findByRole("heading", { name: "Waiting for review" })).toBeInTheDocument();
    expect(screen.getByText("Pham Van Cuong")).toBeInTheDocument();
    expect(screen.getByText("59-X1 123.45")).toBeInTheDocument();
    expect(screen.queryByRole("form")).toBeNull();
    noTrips();
  });

  it("PENDING after a vehicle change: shows the note the service wrote", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("PENDING", { reason: "Vehicle changed: waiting for review" }));
    renderApp();
    expect(await screen.findByText("Vehicle changed: waiting for review")).toBeInTheDocument();
  });

  it("REJECTED: shows the reason and a 'Fix and re-apply' form", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("REJECTED", { reason: "The plate photo is unreadable" }));
    renderApp();
    expect(await screen.findByRole("heading", { name: "Application not approved" })).toBeInTheDocument();
    expect(screen.getByText("The plate photo is unreadable")).toBeInTheDocument();
    expect(screen.getByLabelText("Plate number")).toHaveValue("59-X1 123.45");
    expect(screen.getByRole("button", { name: "Fix and re-apply" })).toBeInTheDocument();
    noTrips();
  });

  it("SUSPENDED: shows the reason, no form and no way to re-apply", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("SUSPENDED", { reason: "Complaints from riders" }));
    renderApp();
    expect(await screen.findByRole("heading", { name: "Account suspended" })).toBeInTheDocument();
    expect(screen.getByText("Complaints from riders")).toBeInTheDocument();
    expect(screen.queryByRole("form")).toBeNull();
    expect(screen.queryByRole("button", { name: /apply/i })).toBeNull();
    noTrips();
  });

  it("APPROVED: the normal app, with a way to edit the vehicle", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("APPROVED"));
    renderApp();
    expect(await screen.findByRole("heading", { name: "You're online" })).toBeInTheDocument();
    expect(screen.getByText("Online")).toBeInTheDocument();
    expect(screen.queryByText(/Couldn't check your driver profile/)).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Your driver profile" }));
    const dialog = screen.getByRole("dialog", { name: "Your driver profile" });
    expect(within(dialog).getByLabelText("Vehicle model")).toHaveValue("Honda City");
    expect(within(dialog).getByRole("note")).toHaveTextContent(/sends your profile back to review/);
  });

  it("changing the plate of an approved driver moves the app to 'Waiting for review'", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("APPROVED"));
    const update = vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("PENDING", { plate: "51A-123.45" }));
    renderApp();
    await screen.findByRole("heading", { name: "You're online" });

    fireEvent.click(screen.getByRole("button", { name: "Your driver profile" }));
    const dialog = screen.getByRole("dialog");
    fireEvent.change(within(dialog).getByLabelText("Plate number"), { target: { value: "51A-123.45" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Save changes" }));
    fireEvent.click(within(dialog).getByRole("button", { name: "Save and send for review" }));

    expect(await screen.findByRole("heading", { name: "Waiting for review" })).toBeInTheDocument();
    expect(update).toHaveBeenCalled();
    expect(screen.queryByRole("dialog")).toBeNull();
    noTrips();
  });

  it("unlocks after an application is approved: submit, wait, check status", async () => {
    const get = vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("NONE"));
    vi.spyOn(client, "updateDriverProfile").mockResolvedValue(profile("NONE"));
    vi.spyOn(client, "applyAsDriver").mockResolvedValue(profile("PENDING"));
    renderApp();
    await screen.findByRole("heading", { name: "Become a driver" });

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "Pham Van Cuong" } });
    fireEvent.change(screen.getByLabelText("Vehicle type"), { target: { value: "CAR" } });
    fireEvent.change(screen.getByLabelText("Plate number"), { target: { value: "59-X1 123.45" } });
    fireEvent.click(screen.getByRole("button", { name: "Submit application" }));
    await screen.findByRole("heading", { name: "Waiting for review" });

    get.mockResolvedValue(profile("APPROVED"));
    fireEvent.click(screen.getByRole("button", { name: "Check status" }));
    expect(await screen.findByRole("heading", { name: "You're online" })).toBeInTheDocument();
  });

  it("profile service down: the normal app with a small notice, not a lock-out", async () => {
    const get = vi.spyOn(client, "getDriverProfile").mockRejectedValue(apiError(503));
    renderApp();
    expect(await screen.findByRole("heading", { name: "You're online" })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Couldn't check your driver profile right now");
    // No profile, so no edit button.
    expect(screen.queryByRole("button", { name: "Your driver profile" })).toBeNull();

    get.mockResolvedValue(profile("APPROVED"));
    fireEvent.click(screen.getByRole("button", { name: "Check again" }));
    await waitFor(() => expect(screen.queryByText(/Couldn't check your driver profile/)).toBeNull());
    expect(screen.getByRole("button", { name: "Your driver profile" })).toBeInTheDocument();
  });

  it("a network failure is also 'unknown', not 'denied'", async () => {
    vi.spyOn(client, "getDriverProfile").mockRejectedValue(new TypeError("Failed to fetch"));
    renderApp();
    expect(await screen.findByRole("heading", { name: "You're online" })).toBeInTheDocument();
  });

  it.each([
    [403, "forbidden"],
    [404, "not_found"],
  ])("a %i from the profile service is a refusal: no app, no trips, and a retry", async (status, code) => {
    const get = vi.spyOn(client, "getDriverProfile").mockRejectedValue(apiError(status, code));
    renderApp();
    expect(await screen.findByRole("heading", { name: "Can't verify your driver account" })).toBeInTheDocument();
    noTrips();

    get.mockResolvedValue(profile("APPROVED"));
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByRole("heading", { name: "You're online" })).toBeInTheDocument();
  });

  it("shows nothing about trips while the profile is still being checked", async () => {
    vi.spyOn(client, "getDriverProfile").mockReturnValue(new Promise(() => {}));
    renderApp();
    expect(await screen.findByRole("heading", { name: "Checking your account…" })).toBeInTheDocument();
    noTrips();
  });

  it("a trip already under way stays visible if the driver is not approved, with a notice", async () => {
    vi.spyOn(client, "getDriverProfile").mockResolvedValue(profile("SUSPENDED", { reason: "Complaints" }));
    vi.spyOn(client, "getActiveTrip").mockResolvedValue({ id: "trip-1", orderId: "order-1", driverId: "driver-9", status: "IN_PROGRESS" });
    vi.spyOn(client, "waitForOrder").mockResolvedValue({
      id: "order-1",
      riderId: "rider-7",
      pickup: { lat: 10.776, lng: 106.7 },
      dropoff: { lat: 10.78, lng: 106.71 },
      vehicleType: "CAR",
      status: "MATCHED",
    });
    renderApp();
    expect(await screen.findByRole("heading", { name: "Trip in progress" })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(/won't be offered new trips/);
    expect(screen.queryByRole("heading", { name: "Account suspended" })).toBeNull();
  });
});
