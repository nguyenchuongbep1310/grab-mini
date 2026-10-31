import { useId } from "react";
import { Button, PriceTag } from "@grab-mini/ui";
import type { PromoState } from "./usePromo";

export interface PromoFieldProps {
  promo: PromoState;
  /** There is a price to check a code against. */
  canApply: boolean;
}

/** "Have a promo code?" under the price: a collapsed link, then an input and Apply; once applied, the code and what it takes off. */
export function PromoField({ promo, canApply }: PromoFieldProps) {
  const inputId = useId();
  const { applied } = promo;

  if (applied) {
    return (
      <div className="promo">
        <div className="promo__applied" role="status">
          <span>
            <strong>{applied.code}</strong> applied · takes off{" "}
            <PriceTag amount={applied.discount} currency={applied.currency} />
          </span>
          <button type="button" className="promo__link" onClick={promo.remove}>
            Remove
          </button>
        </div>
      </div>
    );
  }

  if (!promo.open) {
    return (
      <div className="promo">
        <button type="button" className="promo__link" aria-expanded="false" onClick={() => promo.setOpen(true)}>
          Have a promo code?
        </button>
        {promo.message && (
          <p className="promo__message" role="alert">
            {promo.message}
          </p>
        )}
      </div>
    );
  }

  return (
    <form
      className="promo"
      onSubmit={(e) => {
        e.preventDefault();
        if (canApply && !promo.checking) promo.apply();
      }}
    >
      <label className="promo__label" htmlFor={inputId}>
        Promo code
      </label>
      <div className="promo__row">
        <input
          id={inputId}
          className="promo__input"
          value={promo.code}
          onChange={(e) => promo.setCode(e.target.value)}
          maxLength={32}
          autoCapitalize="characters"
          autoComplete="off"
          spellCheck={false}
          aria-invalid={promo.message ? true : undefined}
          disabled={promo.checking}
        />
        <Button
          type="submit"
          size="sm"
          variant="secondary"
          loading={promo.checking}
          disabled={!canApply || promo.code.trim() === ""}
        >
          Apply
        </Button>
      </div>
      {!canApply && <p className="promo__hint">A code can be checked once the price is known.</p>}
      {promo.message && (
        <p className="promo__message" role="alert">
          {promo.message}
        </p>
      )}
    </form>
  );
}
