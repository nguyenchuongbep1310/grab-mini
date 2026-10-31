import { describe, expect, it } from "vitest";
import { cancelFeeNote } from "./cancelFee";

const policy = (fee: number, graceSeconds: number) => ({ currency: "VND", fee, graceSeconds });

describe("cancelFeeNote", () => {
  it("says nothing when the policy is unknown or cancelling is free", () => {
    expect(cancelFeeNote(null)).toBe("");
    expect(cancelFeeNote(policy(0, 180))).toBe("");
  });

  it("names the free window and the fee", () => {
    const note = cancelFeeNote(policy(10000, 180));
    expect(note).toContain("3 minutes");
    expect(note).toContain("10.000đ");
  });

  it("uses singular and seconds where they read better", () => {
    expect(cancelFeeNote(policy(10000, 60))).toContain("1 minute ");
    expect(cancelFeeNote(policy(10000, 90))).toContain("90 seconds");
  });

  it("without a grace period, the fee applies at once", () => {
    expect(cancelFeeNote(policy(10000, 0))).toBe("Cancelling now costs 10.000đ.");
  });
});
