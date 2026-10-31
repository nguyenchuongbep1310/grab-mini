import { describe, expect, it } from "vitest";
import { emptyForm, validatePromoForm, type PromoFormValues } from "./promoForm";

const NOW = new Date("2026-06-01T00:00:00").getTime();
const form = (over: Partial<PromoFormValues> = {}): PromoFormValues => ({
  ...emptyForm,
  code: "summer20",
  value: "20",
  validTo: "2026-07-01T00:00",
  ...over,
});

function errorsOf(f: PromoFormValues) {
  const r = validatePromoForm(f, NOW);
  if (r.ok) throw new Error("expected errors");
  return r.errors;
}

describe("validatePromoForm", () => {
  it("builds a request with only what was filled in", () => {
    const r = validatePromoForm(form(), NOW);
    expect(r).toEqual({
      ok: true,
      promotion: { code: "SUMMER20", kind: "PERCENT", value: 20, minFare: 0, validTo: new Date("2026-07-01T00:00").toISOString(), perRiderLimit: 1 },
    });
  });

  it("passes the optional fields through", () => {
    const r = validatePromoForm(
      form({ maxDiscount: "20000", minFare: "40000", vehicleType: "BIKE", validFrom: "2026-06-10T08:00", usageLimit: "100", perRiderLimit: "3" }),
      NOW,
    );
    expect(r).toMatchObject({
      ok: true,
      promotion: {
        maxDiscount: 20000,
        minFare: 40000,
        vehicleType: "BIKE",
        validFrom: new Date("2026-06-10T08:00").toISOString(),
        usageLimit: 100,
        perRiderLimit: 3,
      },
    });
  });

  it("checks the code", () => {
    expect(errorsOf(form({ code: "ab" })).code).toBeDefined();
    expect(errorsOf(form({ code: "has space" })).code).toBeDefined();
    expect(errorsOf(form({ code: "x".repeat(33) })).code).toBeDefined();
    expect(validatePromoForm(form({ code: "  A-b_9  " }), NOW)).toMatchObject({ ok: true, promotion: { code: "A-B_9" } });
  });

  it("checks the value by kind", () => {
    expect(errorsOf(form({ value: "" })).value).toBeDefined();
    expect(errorsOf(form({ value: "0" })).value).toBeDefined();
    expect(errorsOf(form({ value: "12.5" })).value).toBeDefined();
    expect(errorsOf(form({ value: "101" })).value).toMatch(/100/);
    expect(validatePromoForm(form({ kind: "FIXED", value: "15000" }), NOW)).toMatchObject({ ok: true });
  });

  it("allows a cap on percent codes only", () => {
    expect(errorsOf(form({ kind: "FIXED", value: "15000", maxDiscount: "5000" })).maxDiscount).toBeDefined();
    expect(errorsOf(form({ maxDiscount: "0" })).maxDiscount).toBeDefined();
  });

  it("checks amounts and limits", () => {
    expect(errorsOf(form({ minFare: "-1" })).minFare).toBeDefined();
    expect(errorsOf(form({ usageLimit: "0" })).usageLimit).toBeDefined();
    expect(errorsOf(form({ perRiderLimit: "0" })).perRiderLimit).toBeDefined();
    expect(errorsOf(form({ perRiderLimit: "" })).perRiderLimit).toBeDefined();
  });

  it("checks the validity window", () => {
    expect(errorsOf(form({ validTo: "" })).validTo).toBeDefined();
    expect(errorsOf(form({ validTo: "2026-05-01T00:00" })).validTo).toMatch(/future/);
    expect(errorsOf(form({ validFrom: "2026-08-01T00:00" })).validTo).toMatch(/after the start/);
  });

  it("reports every problem at once", () => {
    expect(Object.keys(errorsOf(form({ code: "", value: "", validTo: "" }))).sort()).toEqual(["code", "validTo", "value"]);
  });
});
