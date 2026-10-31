import type { NewPromotion, PromotionKind, VehicleType } from "@grab-mini/client";

// The fields of the "new promotion" form, exactly as typed. Checked here so an admin
// sees what is wrong before the request; Promotion Service checks the same rules again.
export interface PromoFormValues {
  code: string;
  kind: PromotionKind;
  value: string;
  maxDiscount: string;
  minFare: string;
  /** "" for any vehicle. */
  vehicleType: "" | VehicleType;
  /** A datetime-local value ("2026-01-31T09:00"); empty means now. */
  validFrom: string;
  validTo: string;
  usageLimit: string;
  perRiderLimit: string;
}

export type PromoFormErrors = Partial<Record<keyof PromoFormValues, string>>;

export const emptyForm: PromoFormValues = {
  code: "",
  kind: "PERCENT",
  value: "",
  maxDiscount: "",
  minFare: "",
  vehicleType: "",
  validFrom: "",
  validTo: "",
  usageLimit: "",
  perRiderLimit: "1",
};

const CODE = /^[A-Z0-9_-]{3,32}$/;
const MAX_AMOUNT = 1_000_000_000_000;

/** A whole number of at least `min`, or null if the text is not one. Empty text is not a number. */
function whole(text: string, min: number): number | null {
  const t = text.trim();
  if (!/^\d+$/.test(t)) return null;
  const n = Number(t);
  return n >= min && n <= MAX_AMOUNT ? n : null;
}

function when(text: string): number {
  return text ? new Date(text).getTime() : NaN;
}

/** Turns the form into a request, or says what is wrong with each field. `now` is for tests. */
export function validatePromoForm(
  f: PromoFormValues,
  now: number = Date.now(),
): { ok: true; promotion: NewPromotion } | { ok: false; errors: PromoFormErrors } {
  const errors: PromoFormErrors = {};
  const code = f.code.trim().toUpperCase();
  if (!CODE.test(code)) errors.code = "3 to 32 characters: letters, digits, - or _.";

  const value = whole(f.value, 1);
  if (value === null) errors.value = f.kind === "PERCENT" ? "A whole percentage from 1 to 100." : "A whole amount in VND above 0.";
  else if (f.kind === "PERCENT" && value > 100) errors.value = "A percentage can't be more than 100.";

  let maxDiscount: number | undefined;
  if (f.maxDiscount.trim() !== "") {
    if (f.kind !== "PERCENT") errors.maxDiscount = "A cap applies to percentage codes only.";
    else {
      const n = whole(f.maxDiscount, 1);
      if (n === null) errors.maxDiscount = "A whole amount in VND above 0, or leave empty for no cap.";
      else maxDiscount = n;
    }
  }

  let minFare = 0;
  if (f.minFare.trim() !== "") {
    const n = whole(f.minFare, 0);
    if (n === null) errors.minFare = "A whole amount in VND, 0 or more.";
    else minFare = n;
  }

  const from = f.validFrom ? when(f.validFrom) : now;
  const to = when(f.validTo);
  if (f.validFrom && Number.isNaN(from)) errors.validFrom = "Not a valid date.";
  if (!f.validTo) errors.validTo = "Choose when the code stops working.";
  else if (Number.isNaN(to)) errors.validTo = "Not a valid date.";
  else if (to <= now) errors.validTo = "The end must be in the future.";
  else if (!Number.isNaN(from) && to <= from) errors.validTo = "The end must be after the start.";

  let usageLimit: number | undefined;
  if (f.usageLimit.trim() !== "") {
    const n = whole(f.usageLimit, 1);
    if (n === null) errors.usageLimit = "A whole number above 0, or leave empty for no limit.";
    else usageLimit = n;
  }

  const perRiderLimit = whole(f.perRiderLimit, 1);
  if (perRiderLimit === null) errors.perRiderLimit = "A whole number, 1 or more.";

  if (Object.keys(errors).length > 0 || value === null || perRiderLimit === null) return { ok: false, errors };

  return {
    ok: true,
    promotion: {
      code,
      kind: f.kind,
      value,
      ...(maxDiscount !== undefined ? { maxDiscount } : {}),
      minFare,
      ...(f.vehicleType ? { vehicleType: f.vehicleType } : {}),
      ...(f.validFrom ? { validFrom: new Date(from).toISOString() } : {}),
      validTo: new Date(to).toISOString(),
      ...(usageLimit !== undefined ? { usageLimit } : {}),
      perRiderLimit,
    },
  };
}
