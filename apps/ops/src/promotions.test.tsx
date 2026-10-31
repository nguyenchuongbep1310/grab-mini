import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { Promotion } from "@grab-mini/client";
import { Promotions } from "./Promotions";

const promo = (over: Partial<Promotion> = {}): Promotion => ({
  code: "WELCOME10",
  kind: "PERCENT",
  value: 10,
  maxDiscount: 20000,
  minFare: 40000,
  vehicleType: null,
  validFrom: "2026-01-01T00:00:00Z",
  validTo: "2099-01-01T00:00:00Z",
  usageLimit: 100,
  perRiderLimit: 1,
  disabledAt: null,
  status: "active",
  redeemed: 7,
  overLimit: 0,
  createdBy: "ops-1",
  createdAt: "2026-01-01T00:00:00Z",
  ...over,
});

afterEach(() => vi.restoreAllMocks());

describe("Promotions panel", () => {
  it("lists promotions with their usage and status", async () => {
    vi.spyOn(client, "listPromotions").mockResolvedValue([
      promo(),
      promo({ code: "OLD5K", kind: "FIXED", value: 5000, maxDiscount: null, minFare: 0, vehicleType: "BIKE", usageLimit: null, status: "expired", redeemed: 3, overLimit: 1 }),
    ]);
    render(<Promotions />);

    const table = await screen.findByRole("table", { name: "Promotions" });
    const rows = within(table).getAllByRole("row");
    expect(rows[1]).toHaveTextContent("WELCOME10");
    expect(rows[1]).toHaveTextContent("10%");
    expect(rows[1]).toHaveTextContent("7 / 100");
    expect(rows[1]).toHaveTextContent("Active");
    expect(rows[2]).toHaveTextContent("5.000đ");
    expect(rows[2]).toHaveTextContent("Bike");
    expect(rows[2]).toHaveTextContent("3 (+1 over)");
    expect(rows[2]).toHaveTextContent("Expired");
    // An expired code has nothing left to disable.
    expect(within(rows[2]).queryByRole("button")).not.toBeInTheDocument();
  });

  it("says so when there are none, and when the list can't be loaded", async () => {
    const list = vi.spyOn(client, "listPromotions").mockResolvedValueOnce([]);
    const { unmount } = render(<Promotions />);
    expect(await screen.findByText("No promotions yet.")).toBeInTheDocument();
    unmount();

    list.mockRejectedValueOnce(new client.ApiError("forbidden", 403, {}));
    render(<Promotions />);
    expect(await screen.findByRole("alert")).toHaveTextContent("admin token");
  });

  it("disables a promotion after a confirmation, then reloads", async () => {
    const list = vi
      .spyOn(client, "listPromotions")
      .mockResolvedValueOnce([promo()])
      .mockResolvedValueOnce([promo({ status: "disabled", disabledAt: "2026-06-01T00:00:00Z" })]);
    const disable = vi.spyOn(client, "disablePromotion").mockResolvedValue(promo({ status: "disabled" }));
    render(<Promotions />);

    fireEvent.click(await screen.findByRole("button", { name: "Disable WELCOME10" }));
    expect(disable).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Keep" }));
    expect(screen.getByRole("button", { name: "Disable WELCOME10" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Disable WELCOME10" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm disable WELCOME10" }));
    await waitFor(() => expect(disable).toHaveBeenCalledWith("WELCOME10"));
    expect(await screen.findByText("Disabled")).toBeInTheDocument();
    expect(list).toHaveBeenCalledTimes(2);
  });

  describe("new promotion", () => {
    const fill = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });

    async function openForm() {
      vi.spyOn(client, "listPromotions").mockResolvedValue([]);
      render(<Promotions />);
      await screen.findByText("No promotions yet.");
      fireEvent.click(screen.getByRole("button", { name: "New promotion" }));
    }

    it("does not send an invalid form and shows each problem next to its field", async () => {
      const create = vi.spyOn(client, "createPromotion");
      await openForm();
      fill("Code", "x");
      fireEvent.click(screen.getByRole("button", { name: "Create promotion" }));

      expect(create).not.toHaveBeenCalled();
      expect(screen.getByLabelText("Code")).toHaveAttribute("aria-invalid", "true");
      expect(screen.getByText(/3 to 32 characters/)).toBeInTheDocument();
      expect(screen.getByText("Choose when the code stops working.")).toBeInTheDocument();
    });

    it("creates a promotion and reloads the list", async () => {
      const create = vi.spyOn(client, "createPromotion").mockResolvedValue(promo({ code: "SUMMER20" }));
      await openForm();
      fill("Code", "summer20");
      fill("Value (%)", "20");
      fill("Max discount (VND)", "30000");
      fill("Vehicle", "CAR");
      fill("Valid until", "2099-01-01T00:00");
      fireEvent.click(screen.getByRole("button", { name: "Create promotion" }));

      await waitFor(() => expect(create).toHaveBeenCalled());
      expect(create.mock.calls[0][0]).toMatchObject({
        code: "SUMMER20",
        kind: "PERCENT",
        value: 20,
        maxDiscount: 30000,
        vehicleType: "CAR",
        perRiderLimit: 1,
      });
      await waitFor(() => expect(screen.queryByRole("form", { name: "New promotion" })).not.toBeInTheDocument());
    });

    it("puts a duplicate code on the code field", async () => {
      vi.spyOn(client, "createPromotion").mockRejectedValue(new client.ApiError("exists", 409, { code: "code_exists" }));
      await openForm();
      fill("Code", "WELCOME10");
      fill("Value (%)", "10");
      fill("Valid until", "2099-01-01T00:00");
      fireEvent.click(screen.getByRole("button", { name: "Create promotion" }));

      expect(await screen.findByText("A promotion with this code already exists.")).toBeInTheDocument();
      expect(screen.getByLabelText("Code")).toHaveAttribute("aria-invalid", "true");
    });

    it("shows the server's reason for a refusal", async () => {
      vi.spyOn(client, "createPromotion").mockRejectedValue(new client.ApiError("validTo is in the past", 400, { code: "invalid_promotion" }));
      await openForm();
      fill("Code", "SUMMER20");
      fill("Value (%)", "20");
      fill("Valid until", "2099-01-01T00:00");
      fireEvent.click(screen.getByRole("button", { name: "Create promotion" }));
      expect(await screen.findByRole("alert")).toHaveTextContent("validTo is in the past");
    });
  });
});
