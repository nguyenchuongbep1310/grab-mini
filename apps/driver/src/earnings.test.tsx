import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as client from "@grab-mini/client";
import type { DriverPayout } from "@grab-mini/client";
import { EarningsLine, summarizePayouts, TripEarnings } from "./Earnings";

const payout = (kind: DriverPayout["kind"], amount: number, commission = 0): DriverPayout => ({
  id: `${kind}-${amount}`,
  kind,
  status: "CAPTURED",
  amount,
  gross: amount + commission,
  commission,
  currency: "VND",
  createdAt: "2026-01-01T00:00:00Z",
});

afterEach(() => vi.restoreAllMocks());

describe("summarizePayouts", () => {
  it("sums the driver's share after commission, fare and tip apart", () => {
    expect(summarizePayouts([payout("FARE", 80000, 20000), payout("TIP", 20000)])).toEqual({
      fare: 80000,
      tip: 20000,
      cancellationFee: 0,
      total: 100000,
      commission: 20000,
    });
  });
});

describe("TripEarnings", () => {
  it("shows what the driver earned for the trip", async () => {
    vi.spyOn(client, "getTripPayments").mockResolvedValue({
      tripId: "t1",
      currency: "VND",
      total: 100000,
      payouts: [payout("FARE", 80000, 20000), payout("TIP", 20000)],
    });
    render(<TripEarnings tripId="t1" refreshKey={0} />);
    const facts = await screen.findByLabelText("Trip earnings");
    expect(facts).toHaveTextContent(/Fare\s*80\.000đ/);
    expect(facts).toHaveTextContent(/Tip\s*20\.000đ/);
    expect(facts).toHaveTextContent(/You earned\s*100\.000đ/);
  });

  it("shows nothing before the trip is settled", async () => {
    const spy = vi.spyOn(client, "getTripPayments").mockResolvedValue({ tripId: "t1", currency: "VND", total: 0, payouts: [] });
    const { container } = render(<TripEarnings tripId="t1" refreshKey={0} />);
    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });
});

describe("EarningsLine", () => {
  const earnings = (over: Partial<client.DriverEarnings>): client.DriverEarnings => ({
    driverId: "driver-1",
    currency: "VND",
    total: 138000,
    fares: 120000,
    tips: 10000,
    cancellationFees: 8000,
    commission: 32000,
    trips: 2,
    payableBalance: 138000,
    ...over,
  });

  it("shows the running total", async () => {
    vi.spyOn(client, "getDriverEarnings").mockResolvedValue(earnings({}));
    render(<EarningsLine driverId="driver-1" refreshKey={0} />);
    expect(await screen.findByText(/Earned 138\.000đ from 2 trips/)).toBeInTheDocument();
  });

  it("stays quiet until there is a trip to report", async () => {
    const spy = vi.spyOn(client, "getDriverEarnings").mockResolvedValue(earnings({ trips: 0, total: 0 }));
    const { container } = render(<EarningsLine driverId="driver-1" refreshKey={0} />);
    await vi.waitFor(() => expect(spy).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });
});
