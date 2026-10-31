import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { DriverApplication, DriverApplicationPage } from "@grab-mini/client";
import { DriverApplications, describeReviewError } from "./DriverApplications";

const app = (over: Partial<DriverApplication> = {}): DriverApplication => ({
  driverId: "driver-9",
  name: "Pham Van Cuong",
  phone: "0901 234 567",
  vehicleType: "CAR",
  plate: "59-X1 123.45",
  model: "Honda City",
  color: "Black",
  status: "PENDING",
  reason: "",
  reviewedAt: null,
  statusAt: "2026-03-01T08:30:00Z",
  updatedAt: "2026-03-01T08:30:00Z",
  reviewedBy: "",
  ...over,
});

const page = (applications: DriverApplication[], nextCursor: string | null = null): DriverApplicationPage => ({ applications, nextCursor });

const apiError = (status: number, code: string, body: Record<string, unknown> = {}) =>
  new client.ApiError(`failed ${status}`, status, { code, ...body });

afterEach(() => vi.restoreAllMocks());


describe("Driver applications panel: the queue", () => {
  it("lists pending applications with name, vehicle, plate, applied time and the phone", async () => {
    const list = vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([app(), app({ driverId: "driver-8", name: "Le Thi Dao", vehicleType: "BIKE", phone: "", model: "", color: "Red", plate: "29-B1 999.99" })]));
    render(<DriverApplications />);

    const table = await screen.findByRole("table", { name: "Pending driver applications" });
    expect(list).toHaveBeenCalledWith({ status: "PENDING", limit: 20 });
    const rows = within(table).getAllByRole("row");
    expect(rows[1]).toHaveTextContent("Pham Van Cuong");
    expect(rows[1]).toHaveTextContent("driver-9");
    expect(rows[1]).toHaveTextContent("0901 234 567 (not verified)");
    expect(rows[1]).toHaveTextContent("Car");
    expect(rows[1]).toHaveTextContent("Honda City · Black");
    expect(rows[1]).toHaveTextContent("59-X1 123.45");
    expect(rows[1]).toHaveTextContent(new Date("2026-03-01T08:30:00Z").toLocaleString([], { dateStyle: "medium", timeStyle: "short" }));
    expect(rows[2]).toHaveTextContent("Bike");
    expect(rows[2]).toHaveTextContent("no phone");
  });

  it("says so when the queue is empty, and when it can't be loaded", async () => {
    const list = vi.spyOn(client, "listDriverApplications").mockResolvedValueOnce(page([]));
    const { unmount } = render(<DriverApplications />);
    expect(await screen.findByText("No applications waiting for review.")).toBeInTheDocument();
    unmount();

    list.mockRejectedValueOnce(new client.ApiError("forbidden", 403, { code: "forbidden" }));
    render(<DriverApplications />);
    expect(await screen.findByRole("alert")).toHaveTextContent("admin token");
  });

  it("pages with the cursor and never shows a driver twice", async () => {
    const list = vi
      .spyOn(client, "listDriverApplications")
      .mockResolvedValueOnce(page([app({ driverId: "driver-1", name: "One" })], "cursor-1"))
      .mockResolvedValueOnce(page([app({ driverId: "driver-1", name: "One" }), app({ driverId: "driver-2", name: "Two" })], null));
    render(<DriverApplications />);

    await screen.findByText("One");
    fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    await screen.findByText("Two");

    expect(list).toHaveBeenLastCalledWith({ status: "PENDING", limit: 20, cursor: "cursor-1" });
    expect(screen.getAllByText("One")).toHaveLength(1);
    // The last page: nothing more to load.
    expect(screen.queryByRole("button", { name: "Load more" })).toBeNull();
  });
});

describe("Driver applications panel: decisions", () => {
  it("approves after a confirmation, then reloads the queue", async () => {
    const list = vi
      .spyOn(client, "listDriverApplications")
      .mockResolvedValueOnce(page([app()]))
      .mockResolvedValueOnce(page([]));
    const review = vi.spyOn(client, "reviewDriver").mockResolvedValue(app({ status: "APPROVED" }));
    render(<DriverApplications />);

    fireEvent.click(await screen.findByRole("button", { name: "Approve driver-9" }));
    expect(review).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Keep" }));
    expect(screen.getByRole("button", { name: "Approve driver-9" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Approve driver-9" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm approve driver-9" }));

    await waitFor(() => expect(review).toHaveBeenCalledWith("driver-9", "approve", undefined));
    expect(await screen.findByText("No applications waiting for review.")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Approved driver-9.");
    expect(list).toHaveBeenCalledTimes(2);
  });

  it("will not reject without a reason, and sends the trimmed reason otherwise", async () => {
    vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([app()]));
    const review = vi.spyOn(client, "reviewDriver").mockResolvedValue(app({ status: "REJECTED" }));
    render(<DriverApplications />);

    fireEvent.click(await screen.findByRole("button", { name: "Reject driver-9" }));
    expect(within(screen.getByRole("table")).getByText(/Don't include personal data about other people/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Confirm reject driver-9" }));
    expect(review).not.toHaveBeenCalled();
    expect(screen.getByText("Give a reason for the driver.")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Reason for rejecting driver-9"), { target: { value: "   " } });
    fireEvent.click(screen.getByRole("button", { name: "Confirm reject driver-9" }));
    expect(review).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Reason for rejecting driver-9"), { target: { value: "  Plate is unreadable  " } });
    fireEvent.click(screen.getByRole("button", { name: "Confirm reject driver-9" }));
    await waitFor(() => expect(review).toHaveBeenCalledWith("driver-9", "reject", "Plate is unreadable"));
    expect(await screen.findByText("Rejected driver-9.")).toBeInTheDocument();
  });

  it("rejects a reason that is too long without calling the service", async () => {
    vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([app()]));
    const review = vi.spyOn(client, "reviewDriver");
    render(<DriverApplications />);

    fireEvent.click(await screen.findByRole("button", { name: "Reject driver-9" }));
    fireEvent.change(screen.getByLabelText("Reason for rejecting driver-9"), { target: { value: "x".repeat(501) } });
    fireEvent.click(screen.getByRole("button", { name: "Confirm reject driver-9" }));
    expect(screen.getByText("At most 500 characters.")).toBeInTheDocument();
    expect(review).not.toHaveBeenCalled();
  });

  it("on 409 invalid_transition shows the status the driver has and refreshes the queue", async () => {
    const list = vi
      .spyOn(client, "listDriverApplications")
      .mockResolvedValueOnce(page([app()]))
      .mockResolvedValueOnce(page([]));
    vi.spyOn(client, "reviewDriver").mockRejectedValue(apiError(409, "invalid_transition", { status: "APPROVED" }));
    render(<DriverApplications />);

    fireEvent.click(await screen.findByRole("button", { name: "Approve driver-9" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm approve driver-9" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("That driver is APPROVED now");
    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    expect(await screen.findByText("No applications waiting for review.")).toBeInTheDocument();
  });

  it("shows other refusals and keeps the row so the admin can retry", async () => {
    vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([app()]));
    vi.spyOn(client, "reviewDriver").mockRejectedValue(new client.ApiError("forbidden", 403, { code: "forbidden" }));
    render(<DriverApplications />);

    fireEvent.click(await screen.findByRole("button", { name: "Approve driver-9" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm approve driver-9" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("admin token");
    expect(screen.getByRole("button", { name: "Approve driver-9" })).toBeInTheDocument();
  });

  it("describes errors for the admin", () => {
    expect(describeReviewError(apiError(409, "invalid_transition", { status: "SUSPENDED" }))).toContain("SUSPENDED");
    expect(describeReviewError(apiError(404, "not_found"))).toBe("No driver with that id has a profile.");
    expect(describeReviewError(apiError(400, "invalid_profile", { field: "reason" }))).toContain("reason");
    expect(describeReviewError(apiError(429, "rate_limited"))).toContain("Too many");
    expect(describeReviewError(new TypeError("Failed to fetch"))).toBe("Failed to fetch");
  });
});

describe("Driver applications panel: suspending by id", () => {
  const fill = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });

  it("needs a driver id and a reason, then a confirmation", async () => {
    const list = vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([]));
    const review = vi.spyOn(client, "reviewDriver").mockResolvedValue(app({ status: "SUSPENDED" }));
    render(<DriverApplications />);
    await screen.findByText("No applications waiting for review.");

    fireEvent.click(screen.getByRole("button", { name: "Suspend" }));
    expect(screen.getByText("Enter the driver id.")).toBeInTheDocument();
    expect(screen.getByText("Give a reason for the driver.")).toBeInTheDocument();
    expect(review).not.toHaveBeenCalled();

    fill("Driver id", " driver-3 ");
    fill("Suspend reason", "Repeated complaints");
    fireEvent.click(screen.getByRole("button", { name: "Suspend" }));
    expect(review).not.toHaveBeenCalled();

    // Editing a field withdraws the confirmation.
    fill("Suspend reason", "Repeated complaints about safety");
    expect(screen.queryByRole("button", { name: /Confirm suspend/ })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Suspend" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm suspend driver-3" }));

    await waitFor(() => expect(review).toHaveBeenCalledWith("driver-3", "suspend", "Repeated complaints about safety"));
    expect(await screen.findByText("Suspended driver-3.")).toBeInTheDocument();
    expect(list).toHaveBeenCalledTimes(2);
    expect(screen.getByLabelText("Driver id")).toHaveValue("");
    expect(screen.getByLabelText("Suspend reason")).toHaveValue("");
  });

  it("keeps the form and shows the status when the driver can't be suspended (409)", async () => {
    vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([]));
    vi.spyOn(client, "reviewDriver").mockRejectedValue(apiError(409, "invalid_transition", { status: "PENDING" }));
    render(<DriverApplications />);
    await screen.findByText("No applications waiting for review.");

    fill("Driver id", "driver-9");
    fill("Suspend reason", "Test");
    fireEvent.click(screen.getByRole("button", { name: "Suspend" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm suspend driver-9" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("That driver is PENDING now");
    expect(screen.getByLabelText("Driver id")).toHaveValue("driver-9");
  });

  it("shows 'not found' for an unknown id", async () => {
    vi.spyOn(client, "listDriverApplications").mockResolvedValue(page([]));
    vi.spyOn(client, "reviewDriver").mockRejectedValue(apiError(404, "not_found"));
    render(<DriverApplications />);
    await screen.findByText("No applications waiting for review.");

    fill("Driver id", "nobody");
    fill("Suspend reason", "Test");
    fireEvent.click(screen.getByRole("button", { name: "Suspend" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm suspend nobody" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("No driver with that id");
  });
});

describe("Driver applications panel: the suspended view", () => {
  it("lists suspended drivers with their reason and reinstates one after a confirmation", async () => {
    const list = vi
      .spyOn(client, "listDriverApplications")
      .mockResolvedValueOnce(page([]))
      .mockResolvedValueOnce(page([app({ status: "SUSPENDED", reason: "Repeated complaints", statusAt: "2026-04-01T00:00:00Z" })]))
      .mockResolvedValueOnce(page([]));
    const review = vi.spyOn(client, "reviewDriver").mockResolvedValue(app({ status: "APPROVED" }));
    render(<DriverApplications />);
    await screen.findByText("No applications waiting for review.");

    fireEvent.change(screen.getByLabelText("Show drivers"), { target: { value: "SUSPENDED" } });
    expect(await screen.findByRole("table", { name: "Suspended drivers" })).toBeInTheDocument();
    expect(list).toHaveBeenLastCalledWith({ status: "SUSPENDED", limit: 20 });
    expect(screen.getByText("Repeated complaints")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Reject driver-9" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Reinstate driver-9" }));
    expect(review).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Confirm reinstate driver-9" }));

    await waitFor(() => expect(review).toHaveBeenCalledWith("driver-9", "approve", undefined));
    expect(await screen.findByText("Reinstated driver-9.")).toBeInTheDocument();
    expect(await screen.findByText("No suspended drivers.")).toBeInTheDocument();
  });
});
