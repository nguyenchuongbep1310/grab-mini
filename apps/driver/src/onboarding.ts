import { ApiError, type DriverProfile, type DriverStatus } from "@grab-mini/client";

// What the Driver App shows depends on the driver's onboarding status. The gate below is a screen, not a
// security boundary: Matching Service is what stops an unapproved driver from being offered trips (the
// `suspended` key and the AVAILABLE status are set only by an APPROVED result). The gate keeps the UI
// honest, so a driver is not told "You're online" when no trip can reach them.

/** How the profile fetch went. */
export type ProfileLoad =
  | { kind: "loading" }
  | { kind: "loaded"; profile: DriverProfile }
  /** The profile service could not be reached or failed (network, 5xx, 429, timeout): we do not know. */
  | { kind: "unavailable" }
  /** A definite refusal (401, 403, 404, 400): this session is not known to the profile service as this driver. */
  | { kind: "denied" };

export type Gate =
  | { kind: "checking" }
  | { kind: "onboarding"; profile: DriverProfile }
  | { kind: "pending"; profile: DriverProfile }
  | { kind: "rejected"; profile: DriverProfile }
  | { kind: "suspended"; profile: DriverProfile }
  /** Approved, or (profile is null) unknown because the profile service is down. */
  | { kind: "app"; profile: DriverProfile | null }
  | { kind: "denied" };

/**
 * Why a failed fetch matters. GET /drivers/{id} answers 200 with status NONE for a driver who never saved
 * anything, so a 404 or 403 is not "no profile" but a real refusal (a wrong identity, a misrouted request):
 * those lock the driver out rather than being treated as approved. Only failures that say nothing about this
 * driver (the service is down, slow or rate limiting) fall back to the normal app, so an existing driver is not
 * locked out of work by an outage; Matching still decides who gets trips.
 */
export function classifyFailure(err: unknown): "unavailable" | "denied" {
  if (err instanceof ApiError && err.status >= 400 && err.status < 500 && err.status !== 408 && err.status !== 429) return "denied";
  return "unavailable";
}

const STATUSES: readonly DriverStatus[] = ["NONE", "PENDING", "APPROVED", "REJECTED", "SUSPENDED"];

/** The screen for a load result. Anything we don't recognise is not approved. */
export function gateOf(load: ProfileLoad): Gate {
  switch (load.kind) {
    case "loading":
      return { kind: "checking" };
    case "unavailable":
      return { kind: "app", profile: null };
    case "denied":
      return { kind: "denied" };
  }
  const { profile } = load;
  if (!STATUSES.includes(profile.status)) return { kind: "denied" };
  switch (profile.status) {
    case "NONE":
      return { kind: "onboarding", profile };
    case "PENDING":
      return { kind: "pending", profile };
    case "REJECTED":
      return { kind: "rejected", profile };
    case "SUSPENDED":
      return { kind: "suspended", profile };
    case "APPROVED":
      return { kind: "app", profile };
  }
}
