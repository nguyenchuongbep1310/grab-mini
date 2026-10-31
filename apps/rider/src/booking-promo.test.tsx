import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { PromoValidation, Quote } from "@grab-mini/client";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";

// Leaflet needs a real browser; the map here is a button that drops the pin.
vi.mock("@grab-mini/ui", async (importOriginal) => {
  const ui = await importOriginal<typeof import("@grab-mini/ui")>();
  return {
    ...ui,
    MapView: ({ onClick }: { onClick?: (p: [number, number]) => void }) => (
      <button type="button" onClick={() => onClick?.([10.78, 106.71])}>
        drop pin
      </button>
    ),
    MapMarker: () => null,
  };
});

const quote: Quote = {
  quoteId: "q1",
  baseFare: 50000,
  multiplier: 1,
  price: 50000,
  currency: "VND",
  expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
};

const validation: PromoValidation = {
  code: "WELCOME10",
  discount: 5000,
  price: 50000,
  finalPrice: 45000,
  currency: "VND",
  promoToken: "tok-1",
  expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
};

beforeEach(() => {
  vi.spyOn(client, "useNotificationSocket").mockReturnValue("open");
  vi.spyOn(client, "getLatestOrder").mockResolvedValue(null);
  vi.spyOn(client, "getQuote").mockResolvedValue(quote);
  vi.spyOn(client, "getRoute").mockRejectedValue(new Error("no routing"));
});
afterEach(() => vi.restoreAllMocks());

async function openWithPrice() {
  render(
    <ToastProvider>
      <App />
    </ToastProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: "drop pin" }));
  await screen.findByText(/50\.000/);
}

async function applyCode() {
  fireEvent.click(screen.getByRole("button", { name: "Have a promo code?" }));
  fireEvent.change(screen.getByLabelText("Promo code"), { target: { value: "WELCOME10" } });
  fireEvent.click(screen.getByRole("button", { name: "Apply" }));
  await screen.findByText(/WELCOME10/, { selector: "strong" });
}

describe("Rider App booking with a promo code", () => {
  it("books without a promo token when no code was applied", async () => {
    const create = vi.spyOn(client, "createOrder").mockResolvedValue({ orderId: "o1", price: 50000 });
    await openWithPrice();
    expect(screen.getByRole("button", { name: "Have a promo code?" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Book ride" }));
    await waitFor(() => expect(create).toHaveBeenCalled());
    expect(create.mock.calls[0].slice(4)).toEqual(["q1", undefined, undefined]);
  });

  it("shows the new total and books with the token", async () => {
    vi.spyOn(client, "validatePromotion").mockResolvedValue(validation);
    const create = vi
      .spyOn(client, "createOrder")
      .mockResolvedValue({ orderId: "o1", price: 50000, discount: 5000, finalPrice: 45000, promoCode: "WELCOME10" });
    await openWithPrice();
    await applyCode();
    expect(await screen.findByText(/45\.000/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Book ride" }));
    await waitFor(() => expect(create).toHaveBeenCalled());
    expect(create.mock.calls[0][6]).toBe("tok-1");
    // Once booked, the sheet still shows what the code saved.
    expect(await screen.findByText(/WELCOME10 saves you/)).toBeInTheDocument();
  });

  it("clears an expired token, keeps the rider on the booking screen and asks to apply again", async () => {
    vi.spyOn(client, "validatePromotion").mockResolvedValue(validation);
    vi.spyOn(client, "createOrder").mockRejectedValue(new client.ApiError("expired", 400, { code: "promo_expired" }));
    await openWithPrice();
    await applyCode();

    fireEvent.click(screen.getByRole("button", { name: "Book ride" }));
    expect(await screen.findAllByText(/Your promo code expired\. Apply it again/)).not.toHaveLength(0);
    expect(screen.getByRole("button", { name: "Apply" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Book ride" })).toBeEnabled();
  });
});
