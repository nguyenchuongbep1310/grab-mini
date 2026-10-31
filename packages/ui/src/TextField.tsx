import { useId, type InputHTMLAttributes, type ReactNode, type SelectHTMLAttributes } from "react";
import { cx } from "./cx";

interface FieldChrome {
  label: string;
  /** A problem with the value; marks the control invalid and is read with it. */
  error?: string;
  /** Help shown under the control while there is no error. */
  hint?: string;
  className?: string;
}

function Field({ id, label, error, hint, className, children }: FieldChrome & { id: string; children: ReactNode }) {
  return (
    <div className={cx("gm-field", error && "gm-field--invalid", className)}>
      <label className="gm-field__label" htmlFor={id}>
        {label}
      </label>
      {children}
      {error ? (
        <span className="gm-field__error" id={`${id}-note`}>
          {error}
        </span>
      ) : hint ? (
        <span className="gm-field__hint" id={`${id}-note`}>
          {hint}
        </span>
      ) : null}
    </div>
  );
}

export interface TextFieldProps extends Omit<InputHTMLAttributes<HTMLInputElement>, "id" | "className">, FieldChrome {}

/** A labelled text input with an optional hint and a field-level error. */
export function TextField({ label, error, hint, className, ...rest }: TextFieldProps) {
  const id = useId();
  const note = error || hint ? `${id}-note` : undefined;
  return (
    <Field id={id} label={label} error={error} hint={hint} className={className}>
      <input id={id} className="gm-field__control" aria-invalid={error ? true : undefined} aria-describedby={note} {...rest} />
    </Field>
  );
}

export interface SelectFieldProps extends Omit<SelectHTMLAttributes<HTMLSelectElement>, "id" | "className">, FieldChrome {}

/** A labelled native select (so it is keyboard and screen reader friendly with no option chosen yet). */
export function SelectField({ label, error, hint, className, children, ...rest }: SelectFieldProps) {
  const id = useId();
  const note = error || hint ? `${id}-note` : undefined;
  return (
    <Field id={id} label={label} error={error} hint={hint} className={className}>
      <select id={id} className="gm-field__control" aria-invalid={error ? true : undefined} aria-describedby={note} {...rest}>
        {children}
      </select>
    </Field>
  );
}
