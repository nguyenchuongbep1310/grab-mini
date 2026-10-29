import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { RiderPayment } from "@grab-mini/client";
import { Receipt, summarize } from "./Receipt";

const pay = (kind: RiderPayment["kind"], amount: number, status: RiderPayment["status"] = "CAPTURED"): RiderPayment => ({
  id: `${kind}-${amount}`,
  kind,
  status,
  amount,
  currency: "VND",
  createdAt: "2026-01-01T00:00:00Z",
});

const response = (payments: RiderPayment[]) => ({
  tripId: "t1",
  currency: "VND",
  total: payments.reduce((s, p) => s + p.amount, 0),
  payments,
});

afterEach(() => vi.restoreAllMocks());

describe("summarize", () => {
  it("adds the fare and the tip into the total", () => {
    expect(summarize([pay("FARE", 100000), pay("TIP", 20000)])).toEqual({
      fare: 100000,
      tip: 20000,
      cancellationFee: null,
      total: 120000,
      declined: false,
    });
  });

  it("does not count a declined payment in the total", () => {
    const lines = summarize([pay("FARE", 100000, "DECLINED"), pay("TIP", 5000)]);
    expect(lines.total).toBe(5000);
    expect(lines.fare).toBeNull();
    expect(lines.declined).toBe(true);
  });

  it("has no lines for a trip nothing was charged for", () => {
    expect(summarize([])).toEqual({ fare: null, tip: null, cancellationFee: null, total: 0, declined: false });
  });
});

describe("Receipt", () => {
  it("shows fare, tip and total once settled", async () => {
    vi.spyOn(client, "getTripPayments").mockResolvedValue(response([pay("FARE", 100000), pay("TIP", 20000)]));
    render(<Receipt tripId="t1" rated tip={20000} />);

    const receipt = await screen.findByLabelText("Receipt");
    expect(receipt).toHaveTextContent(/Fare\s*100\.000đ/);
    expect(receipt).toHaveTextContent(/Tip\s*20\.000đ/);
    expect(receipt).toHaveTextContent(/Total\s*120\.000đ/);
  });

  it("says it is settling until the fare shows up, then shows it", async () => {
    const spy = vi
      .spyOn(client, "getTripPayments")
      .mockResolvedValueOnce(response([]))
      .mockResolvedValue(response([pay("FARE", 45000)]));
    render(<Receipt tripId="t1" rated={false} tip={0} />);

    expect(await screen.findByRole("status")).toHaveTextContent("Settling your payment");
    expect(await screen.findByLabelText("Receipt", {}, { timeout: 4000 })).toHaveTextContent(/Total\s*45\.000đ/);
    expect(spy).toHaveBeenCalledTimes(2);
  });

  it("says the payment was declined when the provider refused it", async () => {
    vi.spyOn(client, "getTripPayments").mockResolvedValue(response([pay("FARE", 100000, "DECLINED")]));
    render(<Receipt tripId="t1" rated={false} tip={0} />);
    expect(await screen.findByLabelText("Receipt")).toHaveTextContent("Payment declined");
  });

  it("renders nothing without a trip", () => {
    const spy = vi.spyOn(client, "getTripPayments");
    const { container } = render(<Receipt tripId={null} rated={false} tip={0} />);
    expect(container).toBeEmptyDOMElement();
    expect(spy).not.toHaveBeenCalled();
  });
});
