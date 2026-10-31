import { Button } from "@grab-mini/ui";
import type { DriverProfile } from "@grab-mini/client";
import { DriverForm } from "./DriverForm";
import type { Gate } from "./onboarding";

export interface StatusScreenProps {
  gate: Exclude<Gate, { kind: "app" }>;
  driverId: string;
  /** Re-reads the profile (the "Check status" button). */
  onRefresh: () => void;
  /** Tries again after the account could not be checked. */
  onRetry: () => void;
  onDone: (profile: DriverProfile) => void;
}

const vehicleLine = (p: DriverProfile) =>
  [p.model, p.color, p.vehicleType === "CAR" ? "Car" : p.vehicleType === "BIKE" ? "Bike" : ""].filter(Boolean).join(" · ");

/** What a driver who is not approved sees instead of "You're online": the form to apply, or where the application stands. */
export function StatusScreen({ gate, driverId, onRefresh, onRetry, onDone }: StatusScreenProps) {
  switch (gate.kind) {
    case "checking":
      return (
        <div role="status">
          <h1 className="sheet-title">Checking your account…</h1>
          <p className="sheet-sub">One moment.</p>
        </div>
      );

    case "denied":
      return (
        <>
          <div>
            <h1 className="sheet-title">Can't verify your driver account</h1>
            <p className="sheet-sub">We couldn't confirm that this session is a driver account, so no trips are shown. Try again, or sign in again.</p>
          </div>
          <Button block onClick={onRetry}>
            Try again
          </Button>
        </>
      );

    case "onboarding":
      return (
        <>
          <div>
            <h1 className="sheet-title">Become a driver</h1>
            <p className="sheet-sub">Tell us about you and your vehicle. An admin reviews every application before you can take trips.</p>
          </div>
          <DriverForm driverId={driverId} initial={gate.profile} mode="apply" submitLabel="Submit application" onDone={onDone} />
        </>
      );

    case "pending":
      return (
        <>
          <div>
            <h1 className="sheet-title">Waiting for review</h1>
            <p className="sheet-sub">
              An admin is looking at your application. You'll start receiving trips as soon as it is approved; this page checks on its own.
            </p>
          </div>
          <dl className="trip-facts">
            <dt>Name</dt>
            <dd>{gate.profile.name}</dd>
            <dt>Vehicle</dt>
            <dd>{vehicleLine(gate.profile) || "…"}</dd>
            <dt>Plate</dt>
            <dd>{gate.profile.plate}</dd>
          </dl>
          {gate.profile.reason && <p className="onboarding__note">{gate.profile.reason}</p>}
          <Button block variant="secondary" onClick={onRefresh}>
            Check status
          </Button>
        </>
      );

    case "rejected":
      return (
        <>
          <div>
            <h1 className="sheet-title">Application not approved</h1>
            <p className="sheet-sub">You are not receiving trips. Fix what the reviewer pointed out and apply again.</p>
          </div>
          <p className="onboarding__note">
            <strong>Reason:</strong> {gate.profile.reason || "No reason was given."}
          </p>
          <DriverForm driverId={driverId} initial={gate.profile} mode="apply" submitLabel="Fix and re-apply" onDone={onDone} />
        </>
      );

    case "suspended":
      return (
        <>
          <div>
            <h1 className="sheet-title">Account suspended</h1>
            <p className="sheet-sub">You can't take trips and can't apply again while your account is suspended. A trip you already started can be finished.</p>
          </div>
          <p className="onboarding__note">
            <strong>Reason:</strong> {gate.profile.reason || "No reason was given."}
          </p>
          <Button block variant="secondary" onClick={onRefresh}>
            Check status
          </Button>
        </>
      );
  }
}
