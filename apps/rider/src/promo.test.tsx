import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, renderHook, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { PromoRejectionCode, PromoValidation, VehicleType } from "@grab-mini/client";
import { PromoField } from "./PromoField";
import { bookingMessage, rejectionMessage, usePromo } from "./usePromo";

const validation = (over: Partial<PromoValidation> = {}): PromoValidation => ({
  code: "WELCOME10",
  discount: 5000,
  price: 50000,
  finalPrice: 45000,
  currency: "VND",
  promoToken: "tok-1",
  expiresAt: "2026-01-01T00:10:00Z",
  ...over,
});

const refusal = (status: number, code: string) => new client.ApiError("refused", status, { code });

afterEach(() => vi.restoreAllMocks());

describe("promo messages", () => {
  it("has a friendly sentence for every rejection and booking code", () => {
    const rejections: PromoRejectionCode[] = ["not_found", "expired", "not_started", "min_fare", "wrong_vehicle", "limit_reached", "already_used"];
    const texts = rejections.map((c) => rejectionMessage(c, "BIKE"));
    expect(new Set(texts).size).toBe(rejections.length);
    for (const t of texts) expect(t).toMatch(/\w+ .*\./);
    expect(rejectionMessage("wrong_vehicle", "BIKE")).toContain("bike");
    expect(rejectionMessage("wrong_vehicle", "CAR")).toContain("car");
    expect(bookingMessage("promo_expired")).toMatch(/Apply it again/);
    expect(bookingMessage("promo_used")).toMatch(/already been used/);
  });
});

describe("usePromo", () => {
  const setup = (price: number | null, vehicle: VehicleType = "CAR") =>
    renderHook(({ p, v }) => usePromo(p, v), { initialProps: { p: price, v: vehicle } });

  it("checks the typed code (upper-cased) for the price and vehicle, and exposes the token", async () => {
    const spy = vi.spyOn(client, "validatePromotion").mockResolvedValue(validation());
    const { result } = setup(50000);
    act(() => result.current.setCode(" welcome10 "));
    act(() => result.current.apply());

    await waitFor(() => expect(result.current.applied?.code).toBe("WELCOME10"));
    expect(spy).toHaveBeenCalledWith("WELCOME10", 50000, "CAR");
    expect(result.current.token).toBe("tok-1");
    expect(result.current.message).toBeNull();
  });

  it("keeps a refused code out and says why", async () => {
    vi.spyOn(client, "validatePromotion").mockRejectedValue(refusal(422, "min_fare"));
    const { result } = setup(20000);
    act(() => result.current.setCode("FLAT15K"));
    act(() => result.current.apply());

    await waitFor(() => expect(result.current.message).toBe(rejectionMessage("min_fare", "CAR")));
    expect(result.current.applied).toBeNull();
    expect(result.current.token).toBeUndefined();
  });

  it("says something neutral when the check itself fails", async () => {
    vi.spyOn(client, "validatePromotion").mockRejectedValue(new TypeError("network"));
    const { result } = setup(20000);
    act(() => result.current.setCode("FLAT15K"));
    act(() => result.current.apply());
    await waitFor(() => expect(result.current.message).toMatch(/Couldn't check the code/));
  });

  it("does not send a token for another price, and checks the code again for the new one", async () => {
    const spy = vi
      .spyOn(client, "validatePromotion")
      .mockResolvedValueOnce(validation())
      .mockResolvedValueOnce(validation({ price: 60000, discount: 6000, finalPrice: 54000, promoToken: "tok-2" }));
    const { result, rerender } = setup(50000);
    act(() => result.current.setCode("WELCOME10"));
    act(() => result.current.apply());
    await waitFor(() => expect(result.current.token).toBe("tok-1"));

    rerender({ p: 60000, v: "CAR" });
    // Right away: the old token is not offered for the new price.
    expect(result.current.token).toBeUndefined();
    await waitFor(() => expect(result.current.token).toBe("tok-2"));
    expect(spy).toHaveBeenLastCalledWith("WELCOME10", 60000, "CAR");
  });

  it("drops the code with a reason when it does not apply to the new vehicle", async () => {
    vi.spyOn(client, "validatePromotion")
      .mockResolvedValueOnce(validation())
      .mockRejectedValueOnce(refusal(422, "wrong_vehicle"));
    const { result, rerender } = setup(50000);
    act(() => result.current.setCode("BIKE5K"));
    act(() => result.current.apply());
    await waitFor(() => expect(result.current.token).toBe("tok-1"));

    rerender({ p: 50000, v: "BIKE" });
    await waitFor(() => expect(result.current.message).toMatch(/no longer applies.*bike/));
    expect(result.current.applied).toBeNull();
    expect(result.current.token).toBeUndefined();
  });

  it("waits while there is no price, then uses the old check if the price came back the same", async () => {
    const spy = vi.spyOn(client, "validatePromotion").mockResolvedValue(validation());
    const { result, rerender } = setup(50000);
    act(() => result.current.setCode("WELCOME10"));
    act(() => result.current.apply());
    await waitFor(() => expect(result.current.token).toBe("tok-1"));

    rerender({ p: null, v: "CAR" });
    expect(result.current.token).toBeUndefined();
    rerender({ p: 50000, v: "CAR" });
    expect(result.current.token).toBe("tok-1");
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("forgets the token when booking refuses it, and asks to apply again", async () => {
    vi.spyOn(client, "validatePromotion").mockResolvedValue(validation());
    const { result } = setup(50000);
    act(() => result.current.setCode("WELCOME10"));
    act(() => result.current.apply());
    await waitFor(() => expect(result.current.token).toBe("tok-1"));

    act(() => result.current.bookingRefused("promo_expired"));
    expect(result.current.token).toBeUndefined();
    expect(result.current.applied).toBeNull();
    expect(result.current.message).toBe(bookingMessage("promo_expired"));
    expect(result.current.code).toBe("WELCOME10");
    expect(result.current.open).toBe(true);
  });

  it("reset puts everything back after a booking", async () => {
    vi.spyOn(client, "validatePromotion").mockResolvedValue(validation());
    const { result } = setup(50000);
    act(() => result.current.setCode("WELCOME10"));
    act(() => result.current.apply());
    await waitFor(() => expect(result.current.token).toBe("tok-1"));
    act(() => result.current.reset());
    expect(result.current).toMatchObject({ applied: null, code: "", open: false, message: null });
  });

  it("cannot apply without a price", () => {
    const spy = vi.spyOn(client, "validatePromotion");
    const { result } = setup(null);
    act(() => result.current.setCode("WELCOME10"));
    act(() => result.current.apply());
    expect(spy).not.toHaveBeenCalled();
  });
});

describe("PromoField", () => {
  // The field wired to the real hook, as the Rider App uses it.
  function Harness({ price }: { price: number | null }) {
    const promo = usePromo(price, "CAR");
    return <PromoField promo={promo} canApply={price !== null} />;
  }

  it("is a collapsed link until opened, then applies a code and shows the discount", async () => {
    vi.spyOn(client, "validatePromotion").mockResolvedValue(validation());
    render(<Harness price={50000} />);

    expect(screen.queryByLabelText("Promo code")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Have a promo code?" }));

    const apply = screen.getByRole("button", { name: "Apply" });
    expect(apply).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Promo code"), { target: { value: "welcome10" } });
    expect(apply).toBeEnabled();
    fireEvent.click(apply);

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("WELCOME10 applied");
    expect(status).toHaveTextContent(/5\.000/);

    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    expect(screen.getByLabelText("Promo code")).toBeInTheDocument();
  });

  it("shows the reason a code was refused", async () => {
    vi.spyOn(client, "validatePromotion").mockRejectedValue(refusal(404, "not_found"));
    render(<Harness price={50000} />);
    fireEvent.click(screen.getByRole("button", { name: "Have a promo code?" }));
    fireEvent.change(screen.getByLabelText("Promo code"), { target: { value: "NOPE" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("We don't recognise that code");
    expect(screen.getByLabelText("Promo code")).toHaveAttribute("aria-invalid", "true");
  });

  it("submits with Enter", async () => {
    const spy = vi.spyOn(client, "validatePromotion").mockResolvedValue(validation());
    render(<Harness price={50000} />);
    fireEvent.click(screen.getByRole("button", { name: "Have a promo code?" }));
    fireEvent.change(screen.getByLabelText("Promo code"), { target: { value: "WELCOME10" } });
    fireEvent.submit(screen.getByLabelText("Promo code").closest("form")!);
    await waitFor(() => expect(spy).toHaveBeenCalledTimes(1));
  });
});
