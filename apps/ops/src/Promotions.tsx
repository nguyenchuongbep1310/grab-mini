import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from "react";
import { Button, Card, StatusBadge } from "@grab-mini/ui";
import {
  ApiError,
  createPromotion,
  disablePromotion,
  listPromotions,
  type Promotion,
} from "@grab-mini/client";
import { emptyForm, validatePromoForm, type PromoFormErrors, type PromoFormValues } from "./promoForm";

const vnd = (n: number) => `${n.toLocaleString("vi-VN")}đ`;
const day = (iso: string) => new Date(iso).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });

/** What an admin reads when a promotion request fails. */
export function describeError(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 401 || err.status === 403) return "This session isn't allowed to manage promotions (an admin token is needed).";
    if (err.status === 409) return "A promotion with this code already exists.";
    if (err.status === 400) return err.message;
  }
  return err instanceof Error ? err.message : "Couldn't reach Promotion Service";
}

const valueOf = (p: Promotion) => (p.kind === "PERCENT" ? `${p.value}%` : vnd(p.value));
const usedOf = (p: Promotion) =>
  `${p.redeemed}${p.usageLimit === null ? "" : ` / ${p.usageLimit}`}${p.overLimit > 0 ? ` (+${p.overLimit} over)` : ""}`;

/** Promotion management for an admin: every code with its usage and status, a form to create one, and disable. */
export function Promotions() {
  const [promotions, setPromotions] = useState<Promotion[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [confirming, setConfirming] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setPromotions(await listPromotions());
      setError(null);
    } catch (err) {
      setError(describeError(err));
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const disable = async (code: string) => {
    setBusy(code);
    try {
      await disablePromotion(code);
      setConfirming(null);
      await load();
    } catch (err) {
      setError(describeError(err));
    } finally {
      setBusy(null);
    }
  };

  return (
    <Card className="promotions">
      <div className="promotions__head">
        <h2>Promotions</h2>
        <Button size="sm" variant={creating ? "ghost" : "secondary"} aria-expanded={creating} onClick={() => setCreating((c) => !c)}>
          {creating ? "Close" : "New promotion"}
        </Button>
      </div>

      {creating && (
        <NewPromotionForm
          onCreated={async () => {
            setCreating(false);
            await load();
          }}
        />
      )}

      {error && (
        <p className="promotions__error" role="alert">
          {error}
        </p>
      )}

      {promotions === null ? (
        !error && <p className="ops__empty">Loading promotions…</p>
      ) : promotions.length === 0 ? (
        <p className="ops__empty">No promotions yet.</p>
      ) : (
        <div className="promotions__scroll">
          <table className="table promotions__table" aria-label="Promotions">
            <thead>
              <tr>
                <th>Code</th>
                <th>Discount</th>
                <th>Applies to</th>
                <th>Valid</th>
                <th>Used</th>
                <th>Per rider</th>
                <th>Status</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {promotions.map((p) => (
                <tr key={p.code}>
                  <td>
                    <strong>{p.code}</strong>
                  </td>
                  <td>
                    {valueOf(p)}
                    {p.kind === "PERCENT" && p.maxDiscount !== null && <div className="promotions__sub">max {vnd(p.maxDiscount)}</div>}
                  </td>
                  <td>
                    {p.vehicleType === null ? "Any vehicle" : p.vehicleType === "BIKE" ? "Bike" : "Car"}
                    {p.minFare > 0 && <div className="promotions__sub">fare {vnd(p.minFare)}+</div>}
                  </td>
                  <td>
                    {day(p.validFrom)}
                    <div className="promotions__sub">to {day(p.validTo)}</div>
                  </td>
                  <td>{usedOf(p)}</td>
                  <td>{p.perRiderLimit}</td>
                  <td>
                    <StatusBadge status={p.status.toUpperCase()} />
                  </td>
                  <td className="promotions__actions">
                    {(p.status === "active" || p.status === "scheduled") &&
                      (confirming === p.code ? (
                        <>
                          <Button size="sm" variant="danger" loading={busy === p.code} onClick={() => disable(p.code)}>
                            Confirm disable {p.code}
                          </Button>
                          <Button size="sm" variant="ghost" disabled={busy === p.code} onClick={() => setConfirming(null)}>
                            Keep
                          </Button>
                        </>
                      ) : (
                        <Button size="sm" variant="secondary" aria-label={`Disable ${p.code}`} onClick={() => setConfirming(p.code)}>
                          Disable
                        </Button>
                      ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function NewPromotionForm({ onCreated }: { onCreated: () => void | Promise<void> }) {
  const [form, setForm] = useState<PromoFormValues>(emptyForm);
  const [errors, setErrors] = useState<PromoFormErrors>({});
  const [failure, setFailure] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const set = <K extends keyof PromoFormValues>(key: K, value: PromoFormValues[K]) => setForm((f) => ({ ...f, [key]: value }));

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setFailure(null);
    const result = validatePromoForm(form);
    if (!result.ok) {
      setErrors(result.errors);
      return;
    }
    setErrors({});
    setSaving(true);
    try {
      await createPromotion(result.promotion);
      await onCreated();
    } catch (err) {
      const message = describeError(err);
      // A code that already exists belongs to the code field; anything else to the form.
      if (err instanceof ApiError && err.status === 409) setErrors({ code: message });
      else setFailure(message);
    } finally {
      setSaving(false);
    }
  };

  const field = (key: keyof PromoFormValues, label: string, input: (id: string) => ReactNode, hint?: string) => {
    const id = `promo-${key}`;
    return (
      <div className="promo-form__field">
        <label htmlFor={id}>{label}</label>
        {input(id)}
        {hint && !errors[key] && <span className="promo-form__hint">{hint}</span>}
        {errors[key] && (
          <span className="promo-form__error" id={`${id}-error`}>
            {errors[key]}
          </span>
        )}
      </div>
    );
  };
  const props = (key: keyof PromoFormValues) => ({
    "aria-invalid": errors[key] ? true : undefined,
    "aria-describedby": errors[key] ? `promo-${key}-error` : undefined,
  });
  const text = (key: Exclude<keyof PromoFormValues, "kind" | "vehicleType">, inputMode?: "numeric") => (id: string) => (
    <input id={id} value={form[key]} inputMode={inputMode} onChange={(e) => set(key, e.target.value)} {...props(key)} />
  );

  return (
    <form className="promo-form" onSubmit={submit} noValidate aria-label="New promotion">
      {field("code", "Code", (id) => (
        <input id={id} value={form.code} maxLength={32} autoComplete="off" onChange={(e) => set("code", e.target.value.toUpperCase())} {...props("code")} />
      ))}
      {field("kind", "Kind", (id) => (
        <select id={id} value={form.kind} onChange={(e) => set("kind", e.target.value as PromoFormValues["kind"])}>
          <option value="PERCENT">Percent</option>
          <option value="FIXED">Fixed amount</option>
        </select>
      ))}
      {field("value", form.kind === "PERCENT" ? "Value (%)" : "Value (VND)", text("value", "numeric"))}
      {field("maxDiscount", "Max discount (VND)", text("maxDiscount", "numeric"), form.kind === "PERCENT" ? "Optional cap" : "Percent codes only")}
      {field("minFare", "Min fare (VND)", text("minFare", "numeric"), "Optional")}
      {field("vehicleType", "Vehicle", (id) => (
        <select id={id} value={form.vehicleType} onChange={(e) => set("vehicleType", e.target.value as PromoFormValues["vehicleType"])}>
          <option value="">Any vehicle</option>
          <option value="CAR">Car</option>
          <option value="BIKE">Bike</option>
        </select>
      ))}
      {field("validFrom", "Valid from", (id) => (
        <input id={id} type="datetime-local" value={form.validFrom} onChange={(e) => set("validFrom", e.target.value)} {...props("validFrom")} />
      ), "Empty means now")}
      {field("validTo", "Valid until", (id) => (
        <input id={id} type="datetime-local" value={form.validTo} onChange={(e) => set("validTo", e.target.value)} {...props("validTo")} />
      ))}
      {field("usageLimit", "Total uses", text("usageLimit", "numeric"), "Optional. Orders in flight can pass it")}
      {field("perRiderLimit", "Uses per rider", text("perRiderLimit", "numeric"))}

      <div className="promo-form__actions">
        {failure && (
          <p className="promotions__error" role="alert">
            {failure}
          </p>
        )}
        <Button type="submit" loading={saving}>
          Create promotion
        </Button>
      </div>
    </form>
  );
}
