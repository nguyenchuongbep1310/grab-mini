import { useCallback, useEffect, useRef, useState } from "react";
import {
  promoBookingErrorOf,
  promoRejectionOf,
  validatePromotion,
  type PromoBookingErrorCode,
  type PromoRejectionCode,
  type PromoValidation,
  type VehicleType,
} from "@grab-mini/client";

const VEHICLE_NAME: Record<VehicleType, string> = { CAR: "car", BIKE: "bike" };

/** What the rider reads when a code is refused. Exported so tests can walk every code. */
export function rejectionMessage(code: PromoRejectionCode, vehicleType: VehicleType): string {
  switch (code) {
    case "not_found":
      return "We don't recognise that code. Check it and try again.";
    case "expired":
      return "This code has expired.";
    case "not_started":
      return "This code isn't active yet.";
    case "min_fare":
      return "This trip is below the minimum fare for this code.";
    case "wrong_vehicle":
      return `This code doesn't apply to ${VEHICLE_NAME[vehicleType]} rides.`;
    case "limit_reached":
      return "This code has been fully redeemed.";
    case "already_used":
      return "You've already used this code.";
  }
}

/** What the rider reads when booking refused the promo token. In every case the code has to be applied again. */
export function bookingMessage(code: PromoBookingErrorCode): string {
  switch (code) {
    case "promo_expired":
      return "Your promo code expired. Apply it again to use it.";
    case "promo_used":
      return "This code has already been used on another ride.";
    case "promo_wrong_price":
      return "The price changed, so your promo code needs checking again. Apply it again.";
    case "promo_wrong_vehicle":
      return "Your promo code was for a different vehicle. Apply it again.";
    case "promo_wrong_rider":
    case "promo_invalid":
      return "Your promo code is no longer valid. Apply it again.";
  }
}

function failureMessage(err: unknown, vehicleType: VehicleType): string {
  const rejection = promoRejectionOf(err);
  return rejection ? rejectionMessage(rejection, vehicleType) : "Couldn't check the code right now. Try again.";
}

interface Applied {
  validation: PromoValidation;
  /** The price it was checked for (the server's own copy could differ only by rounding). */
  price: number;
  vehicleType: VehicleType;
}

export interface PromoState {
  /** The field is expanded (the rider tapped "Have a promo code?" or a code was applied). */
  open: boolean;
  setOpen: (open: boolean) => void;
  code: string;
  setCode: (code: string) => void;
  /** A check is under way. */
  checking: boolean;
  /** Why the last check or booking failed; null when there is nothing to say. */
  message: string | null;
  /** The code in force for the trip on screen: null when none, or when the price or vehicle moved since it was checked. */
  applied: PromoValidation | null;
  /** Check the typed code against the trip on screen. */
  apply: () => void;
  /** Drop the applied code. */
  remove: () => void;
  /** The token to book with: only ever one that matches the price and vehicle on screen. */
  token: string | undefined;
  /** Booking refused the token: forget it and say why. */
  bookingRefused: (code: PromoBookingErrorCode) => void;
  /** Booking succeeded: the code is spent, start clean. */
  reset: () => void;
}

/**
 * A promo code for the trip being planned. The server's token is bound to the price and
 * vehicle it was issued for, so the moment either differs from what was checked the
 * discount stops being shown and is not sent; once a price is available again the code
 * is checked again by itself, and if it no longer applies it is dropped with a reason.
 * `price` is null while no quote is available (the code cannot be checked then).
 */
export function usePromo(price: number | null, vehicleType: VehicleType): PromoState {
  const [open, setOpen] = useState(false);
  const [code, setCode] = useState("");
  const [checking, setChecking] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [held, setHeld] = useState<Applied | null>(null);
  // Only the newest check may change the state.
  const seq = useRef(0);

  const rounded = price === null ? null : Math.round(price);
  const current =
    held && rounded !== null && held.price === rounded && held.vehicleType === vehicleType ? held.validation : null;

  const check = useCallback((raw: string, forPrice: number, forVehicle: VehicleType, recheck: boolean) => {
    const mine = ++seq.current;
    setChecking(true);
    setMessage(null);
    validatePromotion(raw, forPrice, forVehicle)
      .then((validation) => {
        if (mine !== seq.current) return;
        setHeld({ validation, price: forPrice, vehicleType: forVehicle });
        setCode(validation.code);
        setOpen(true);
      })
      .catch((err) => {
        if (mine !== seq.current) return;
        setHeld(null);
        const why = failureMessage(err, forVehicle);
        setMessage(recheck ? `${raw} no longer applies to this trip. ${why}` : why);
      })
      .finally(() => {
        if (mine === seq.current) setChecking(false);
      });
  }, []);

  // The trip moved under an applied code: check it again for the new price and vehicle.
  useEffect(() => {
    if (!held || current || rounded === null) return;
    check(held.validation.code, rounded, vehicleType, true);
  }, [held, current, rounded, vehicleType, check]);

  const apply = useCallback(() => {
    const typed = code.trim().toUpperCase();
    if (!typed || rounded === null) return;
    check(typed, rounded, vehicleType, false);
  }, [code, rounded, vehicleType, check]);

  const remove = useCallback(() => {
    seq.current++;
    setHeld(null);
    setChecking(false);
    setMessage(null);
  }, []);

  const bookingRefused = useCallback((why: PromoBookingErrorCode) => {
    seq.current++;
    setHeld(null);
    setChecking(false);
    setOpen(true);
    setMessage(bookingMessage(why));
  }, []);

  const reset = useCallback(() => {
    seq.current++;
    setHeld(null);
    setChecking(false);
    setMessage(null);
    setCode("");
    setOpen(false);
  }, []);

  return {
    open,
    setOpen,
    code,
    setCode,
    checking,
    message,
    applied: current,
    apply,
    remove,
    token: current?.promoToken,
    bookingRefused,
    reset,
  };
}
