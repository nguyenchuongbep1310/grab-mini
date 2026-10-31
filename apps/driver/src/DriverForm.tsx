import { useState, type FormEvent } from "react";
import { Button, SelectField, TextField } from "@grab-mini/ui";
import {
  applyAsDriver,
  profileFailure,
  toDriverInput,
  updateDriverProfile,
  validateDriverForm,
  vehicleChanged,
  type DriverFormErrors,
  type DriverFormValues,
  type DriverProfile,
} from "@grab-mini/client";

export interface DriverFormProps {
  driverId: string;
  /** What is stored (empty fields for a driver who never saved anything). */
  initial: DriverProfile | null;
  /**
   * apply: save the details, then submit them for review (NONE and REJECTED drivers).
   * edit: change the details of an approved driver; a new plate or vehicle type sends them back to review.
   */
  mode: "apply" | "edit";
  submitLabel: string;
  /** The profile the service returned (its status tells which screen comes next). */
  onDone: (profile: DriverProfile) => void;
}

const FIELDS = ["name", "phone", "vehicleType", "plate", "model", "color"] as const;

const valuesOf = (p: DriverProfile | null): DriverFormValues => ({
  name: p?.name ?? "",
  phone: p?.phone ?? "",
  vehicleType: p?.vehicleType ?? "",
  plate: p?.plate ?? "",
  model: p?.model ?? "",
  color: p?.color ?? "",
});

export function DriverForm({ driverId, initial, mode, submitLabel, onDone }: DriverFormProps) {
  const [form, setForm] = useState<DriverFormValues>(() => valuesOf(initial));
  const [errors, setErrors] = useState<DriverFormErrors>({});
  const [failure, setFailure] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  // An approved driver who changes plate or vehicle type must confirm that they will stop receiving trips.
  const [confirming, setConfirming] = useState(false);

  const set = <K extends keyof DriverFormValues>(key: K, value: DriverFormValues[K]) => {
    setForm((f) => ({ ...f, [key]: value }));
    setConfirming(false);
  };

  const run = async () => {
    setSaving(true);
    setFailure(null);
    try {
      const input = toDriverInput(form);
      const saved = await updateDriverProfile(driverId, input);
      // Applying is a second call. If it fails the details are already saved, and sending both again is harmless
      // (an application that is already PENDING or APPROVED is returned unchanged).
      onDone(mode === "apply" ? await applyAsDriver(driverId) : saved);
    } catch (err) {
      const { field, message } = profileFailure(err);
      if (field && (FIELDS as readonly string[]).includes(field)) setErrors({ [field]: message });
      else setFailure(message);
      setConfirming(false);
    } finally {
      setSaving(false);
    }
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (saving) return;
    const found = validateDriverForm(form);
    setErrors(found);
    setFailure(null);
    if (Object.keys(found).length > 0) return;
    if (mode === "edit" && initial && !confirming && vehicleChanged(initial, form)) {
      setConfirming(true);
      return;
    }
    run();
  };

  return (
    <form className="sheet-stack" onSubmit={submit} noValidate aria-label={mode === "apply" ? "Driver application" : "Your driver profile"}>
      <TextField label="Name" value={form.name} autoComplete="name" maxLength={120} onChange={(e) => set("name", e.target.value)} error={errors.name} />
      <TextField
        label="Phone"
        type="tel"
        value={form.phone}
        autoComplete="tel"
        maxLength={40}
        onChange={(e) => set("phone", e.target.value)}
        error={errors.phone}
        hint="Optional. A reviewer may use it to contact you. It is not verified."
      />
      <SelectField
        label="Vehicle type"
        value={form.vehicleType}
        onChange={(e) => set("vehicleType", e.target.value as DriverFormValues["vehicleType"])}
        error={errors.vehicleType}
      >
        <option value="">Choose…</option>
        <option value="CAR">Car</option>
        <option value="BIKE">Bike</option>
      </SelectField>
      <TextField label="Plate number" value={form.plate} autoCapitalize="characters" autoComplete="off" maxLength={40} onChange={(e) => set("plate", e.target.value)} error={errors.plate} hint="For example 51A-123.45" />
      <TextField label="Vehicle model" value={form.model} autoComplete="off" maxLength={80} onChange={(e) => set("model", e.target.value)} error={errors.model} hint="Optional. For example Toyota Vios" />
      <TextField label="Vehicle colour" value={form.color} autoComplete="off" maxLength={50} onChange={(e) => set("color", e.target.value)} error={errors.color} hint="Optional" />

      {mode === "edit" && (
        <p className="onboarding__warning" role="note">
          Changing the plate number or the vehicle type sends your profile back to review. You will not be offered trips until an admin approves the new vehicle. Model and colour can be changed freely.
        </p>
      )}

      {confirming && (
        <p className="onboarding__warning" role="alert">
          You changed the plate or vehicle type. Saving takes you offline until an admin approves it again.
        </p>
      )}

      {failure && (
        <p className="onboarding__error" role="alert">
          {failure}
        </p>
      )}

      <Button type="submit" block loading={saving} variant={confirming ? "danger" : "primary"}>
        {confirming ? "Save and send for review" : submitLabel}
      </Button>
      {confirming && (
        <Button type="button" block variant="secondary" disabled={saving} onClick={() => setConfirming(false)}>
          Keep editing
        </Button>
      )}
    </form>
  );
}
