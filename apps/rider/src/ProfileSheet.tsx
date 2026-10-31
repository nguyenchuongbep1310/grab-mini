import { useState, type FormEvent } from "react";
import { Button, FormSheet, TextField } from "@grab-mini/ui";
import {
  profileFailure,
  toRiderInput,
  updateRiderProfile,
  validateRiderForm,
  type RiderFormErrors,
  type RiderFormValues,
  type RiderProfile,
} from "@grab-mini/client";
import type { ProfileLoad } from "./useRiderProfile";

export interface ProfileSheetProps {
  open: boolean;
  riderId: string;
  profile: RiderProfile | null;
  status: ProfileLoad;
  onRetry: () => void;
  onSaved: (profile: RiderProfile) => void;
  onClose: () => void;
}

/** The rider's name and phone. The phone is optional and is stored as typed. */
export function ProfileSheet({ open, riderId, profile, status, onRetry, onSaved, onClose }: ProfileSheetProps) {
  return (
    <FormSheet open={open} title="Your profile" onClose={onClose}>
      {status === "loading" && profile === null ? (
        <p className="sheet-sub" role="status">
          Loading your profile…
        </p>
      ) : (
        // Mounted when the sheet opens and when the load finishes, so the fields start from what is stored
        // without an effect that could overwrite what the rider has already typed.
        <ProfileForm riderId={riderId} profile={profile} loadFailed={status === "error" && profile === null} onRetry={onRetry} onSaved={onSaved} />
      )}
    </FormSheet>
  );
}

interface ProfileFormProps {
  riderId: string;
  profile: RiderProfile | null;
  loadFailed: boolean;
  onRetry: () => void;
  onSaved: (profile: RiderProfile) => void;
}

function ProfileForm({ riderId, profile, loadFailed, onRetry, onSaved }: ProfileFormProps) {
  const [form, setForm] = useState<RiderFormValues>({ name: profile?.name ?? "", phone: profile?.phone ?? "" });
  const [errors, setErrors] = useState<RiderFormErrors>({});
  const [failure, setFailure] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const set = (key: keyof RiderFormValues, value: string) => {
    setForm((f) => ({ ...f, [key]: value }));
    setSaved(false);
  };

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setFailure(null);
    setSaved(false);
    const found = validateRiderForm(form);
    setErrors(found);
    if (Object.keys(found).length > 0) return;
    setSaving(true);
    try {
      const next = await updateRiderProfile(riderId, toRiderInput(form));
      onSaved(next);
      setSaved(true);
    } catch (err) {
      const { field, message } = profileFailure(err);
      if (field === "name" || field === "phone") setErrors({ [field]: message });
      else setFailure(message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <form className="sheet-stack" onSubmit={submit} noValidate aria-label="Your profile">
      {loadFailed && (
        <p className="profile__notice" role="status">
          Couldn't load your saved profile.{" "}
          <button type="button" className="promo__link" onClick={onRetry}>
            Try again
          </button>
        </p>
      )}
      <TextField label="Name" value={form.name} autoComplete="name" maxLength={120} onChange={(e) => set("name", e.target.value)} error={errors.name} />
      <TextField
        label="Phone"
        type="tel"
        value={form.phone}
        autoComplete="tel"
        maxLength={40}
        onChange={(e) => set("phone", e.target.value)}
        error={errors.phone}
        hint="Optional. Saved as you type it; it is not checked or verified."
      />
      {failure && (
        <p className="promo__message" role="alert">
          {failure}
        </p>
      )}
      {saved && (
        <p className="promo__applied-note" role="status">
          Profile saved.
        </p>
      )}
      <Button type="submit" block loading={saving}>
        Save profile
      </Button>
    </form>
  );
}
