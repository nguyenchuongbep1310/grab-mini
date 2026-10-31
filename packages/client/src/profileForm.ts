import { ApiError, profileErrorOf, profileFieldOf, type DriverProfileInput, type RiderProfileInput } from "./api";
import type { VehicleType } from "@grab-mini/events";

// Form-side checks for the profile screens (rider profile, driver onboarding). They mirror Profile Service's
// rules loosely so the person gets an answer next to the field without a round trip; the service stays the
// authority (it normalises and re-checks everything), and a refusal from it is mapped back to a field by
// `profileFailure`. The rules are deliberately lenient: they stop garbage, not unusual names or plates.
// Phone numbers are NOT verified anywhere: these checks only look at the shape of what was typed.

/** Control characters (tab and newline included), line and paragraph separators, bidi overrides and isolates. Built from a string so the source holds no raw separator characters. */
const FORBIDDEN = new RegExp("[\\p{Cc}\\u2028\\u2029\\u202A-\\u202E\\u2066-\\u2069]", "u");

/** Trimmed, with runs of white space collapsed to one space (what the service stores). */
export function tidy(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

/** Plates are stored in capitals. */
export function normalizePlate(text: string): string {
  return tidy(text).toUpperCase();
}

const length = (text: string) => [...text].length;

export const PROFILE_LIMITS = { name: 80, phone: 32, plateMin: 3, plateMax: 16, model: 50, color: 30 } as const;

export function validateName(raw: string): string | null {
  const text = tidy(raw);
  if (FORBIDDEN.test(raw.trim())) return "Use letters, numbers and normal punctuation only.";
  if (text === "") return "Enter your name.";
  if (length(text) > PROFILE_LIMITS.name) return `At most ${PROFILE_LIMITS.name} characters.`;
  if (!/\p{L}/u.test(text)) return "A name needs at least one letter.";
  return null;
}

/** Optional. Digits with spaces, dots, dashes, brackets and a leading +; 7 to 15 digits. */
export function validatePhone(raw: string): string | null {
  const text = tidy(raw);
  if (text === "") return null;
  if (FORBIDDEN.test(raw.trim()) || !/^\+?[\d\s.\-()]+$/.test(text)) return "Use digits, spaces, dashes and brackets, with an optional leading +.";
  const digits = text.replace(/\D/g, "").length;
  if (digits < 7 || digits > 15) return "A phone number has 7 to 15 digits.";
  return null;
}

export function validatePlate(raw: string): string | null {
  const text = normalizePlate(raw);
  if (FORBIDDEN.test(raw.trim())) return "Use letters, numbers, spaces, - and . only.";
  if (text === "") return "Enter the plate number.";
  if (length(text) < PROFILE_LIMITS.plateMin || length(text) > PROFILE_LIMITS.plateMax) return `${PROFILE_LIMITS.plateMin} to ${PROFILE_LIMITS.plateMax} characters.`;
  if (!/^[\p{L}\p{N} .\-]+$/u.test(text)) return "Use letters, numbers, spaces, - and . only.";
  if (text.replace(/[^\p{L}\p{N}]/gu, "").length < PROFILE_LIMITS.plateMin) return "A plate needs at least 3 letters or digits.";
  return null;
}

/** Model and colour are optional descriptions. */
export function validateDescription(raw: string, max: number): string | null {
  if (FORBIDDEN.test(raw.trim())) return "Use letters, numbers and normal punctuation only.";
  if (length(tidy(raw)) > max) return `At most ${max} characters.`;
  return null;
}

export interface RiderFormValues {
  name: string;
  phone: string;
}

export type RiderFormErrors = Partial<Record<keyof RiderFormValues, string>>;

export function validateRiderForm(v: RiderFormValues): RiderFormErrors {
  const errors: RiderFormErrors = {};
  const name = validateName(v.name);
  if (name) errors.name = name;
  const phone = validatePhone(v.phone);
  if (phone) errors.phone = phone;
  return errors;
}

export function toRiderInput(v: RiderFormValues): RiderProfileInput {
  return { name: tidy(v.name), phone: tidy(v.phone) };
}

export interface DriverFormValues extends RiderFormValues {
  vehicleType: VehicleType | "";
  plate: string;
  model: string;
  color: string;
}

export type DriverFormErrors = Partial<Record<keyof DriverFormValues, string>>;

/** `requireVehicle`: applying needs a vehicle type and a plate; a draft only needs the name. */
export function validateDriverForm(v: DriverFormValues, requireVehicle = true): DriverFormErrors {
  const errors: DriverFormErrors = validateRiderForm(v);
  if (requireVehicle && v.vehicleType === "") errors.vehicleType = "Choose Car or Bike.";
  if (requireVehicle || tidy(v.plate) !== "") {
    const plate = validatePlate(v.plate);
    if (plate) errors.plate = plate;
  }
  const model = validateDescription(v.model, PROFILE_LIMITS.model);
  if (model) errors.model = model;
  const color = validateDescription(v.color, PROFILE_LIMITS.color);
  if (color) errors.color = color;
  return errors;
}

export function toDriverInput(v: DriverFormValues): DriverProfileInput {
  return {
    name: tidy(v.name),
    phone: tidy(v.phone),
    ...(v.vehicleType === "" ? {} : { vehicleType: v.vehicleType }),
    plate: normalizePlate(v.plate),
    model: tidy(v.model),
    color: tidy(v.color),
  };
}

/** Did the plate or vehicle type change? For an approved driver that sends the profile back to review. */
export function vehicleChanged(before: { vehicleType: string; plate: string }, after: { vehicleType: string; plate: string }): boolean {
  return before.vehicleType !== after.vehicleType || normalizePlate(before.plate) !== normalizePlate(after.plate);
}

/** A refusal from Profile Service as the form shows it: a field to mark (or null for the whole form) and what to say. */
export interface ProfileFailure {
  field: string | null;
  message: string;
}

export function profileFailure(err: unknown): ProfileFailure {
  const code = profileErrorOf(err);
  const field = profileFieldOf(err);
  switch (code) {
    case "invalid_profile":
    case "incomplete_profile":
      return { field, message: err instanceof Error && err.message ? err.message : "Check this field." };
    case "forbidden":
      return { field: null, message: "This session isn't allowed to change that profile." };
    case "suspended":
      return { field: null, message: "Your driver account is suspended, so you can't apply." };
    case "rate_limited":
      return { field: null, message: "Too many requests. Wait a moment and try again." };
    default:
      return { field: null, message: err instanceof ApiError || err instanceof Error ? err.message : "Couldn't reach the profile service." };
  }
}
