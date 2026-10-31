import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { Button, Card, TextField } from "@grab-mini/ui";
import {
  ApiError,
  listDriverApplications,
  profileErrorOf,
  reviewDriver,
  type DriverApplication,
  type ReviewAction,
} from "@grab-mini/client";

const PAGE_SIZE = 20;
const MAX_REASON = 500;
const when = (iso: string | null) => (iso ? new Date(iso).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }) : "");
const countChars = (s: string) => [...s].length;

const VERB: Record<ReviewAction, string> = { approve: "Approved", reject: "Rejected", suspend: "Suspended" };

/** The two lists: applications waiting for a decision, and suspended drivers (whom an admin can approve again). */
type View = "PENDING" | "SUSPENDED";

/** What an admin reads when a review or a list request fails. */
export function describeReviewError(err: unknown): string {
  const code = profileErrorOf(err);
  if (err instanceof ApiError) {
    if (code === "invalid_transition") {
      const status = typeof err.body.status === "string" ? err.body.status : "";
      return status
        ? `That driver is ${status} now, so this action isn't allowed. The list was refreshed.`
        : "That driver's status changed, so this action isn't allowed. The list was refreshed.";
    }
    if (code === "not_found") return "No driver with that id has a profile.";
    if (code === "invalid_profile") return "The reason isn't valid (1 to 500 characters, no control characters).";
    if (err.status === 401 || err.status === 403) return "This session isn't allowed to review drivers (an admin token is needed).";
    if (code === "rate_limited") return "Too many requests. Wait a moment and try again.";
  }
  return err instanceof Error ? err.message : "Couldn't reach Profile Service";
}

/** The reason is shown to the driver and kept with the decision, so the form says what not to put in it. */
const REASON_NOTE = "The driver reads this reason. Don't include personal data about other people.";

type Pending = { id: string; action: ReviewAction } | null;

/**
 * Admin review of driver applications: the PENDING queue (oldest first, with a Load more through the cursor),
 * Approve and Reject on each row, and a form to suspend an approved driver by id. The "Suspended" view lists
 * suspended drivers with a Reinstate action (the API allows SUSPENDED to APPROVED), so a suspension isn't a dead end. Every decision takes two
 * steps (a confirm button, like disabling a promotion) and the queue is re-read afterwards. Reject and suspend
 * need a reason. Names, phones and plates appear on this screen for the reviewer and nowhere else here.
 */
export function DriverApplications() {
  const [view, setView] = useState<View>("PENDING");
  const [rows, setRows] = useState<DriverApplication[] | null>(null);
  const [next, setNext] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  // The row action being confirmed, and (for reject) the reason typed so far.
  const [confirming, setConfirming] = useState<Pending>(null);
  const [reason, setReason] = useState("");
  const [reasonError, setReasonError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ kind: "ok" | "error"; text: string } | null>(null);
  // Ignore an answer to a list request that a newer one has replaced.
  const generation = useRef(0);

  const load = useCallback(async () => {
    const mine = ++generation.current;
    try {
      const page = await listDriverApplications({ status: view, limit: PAGE_SIZE });
      if (mine !== generation.current) return;
      setRows(page.applications);
      setNext(page.nextCursor);
      setLoadError(null);
    } catch (err) {
      if (mine === generation.current) setLoadError(describeReviewError(err));
    }
  }, [view]);

  useEffect(() => {
    setRows(null);
    setNext(null);
    setConfirming(null);
    load();
  }, [load]);

  const loadMore = async () => {
    if (!next) return;
    const mine = generation.current;
    setLoadingMore(true);
    try {
      const page = await listDriverApplications({ status: view, limit: PAGE_SIZE, cursor: next });
      if (mine !== generation.current) return;
      setRows((r) => {
        // The cursor is stable, but a refresh may have raced: never show a driver twice.
        const seen = new Set((r ?? []).map((a) => a.driverId));
        return [...(r ?? []), ...page.applications.filter((a) => !seen.has(a.driverId))];
      });
      setNext(page.nextCursor);
    } catch (err) {
      setLoadError(describeReviewError(err));
    } finally {
      setLoadingMore(false);
    }
  };

  /** Runs a decision; on success or on a conflict the queue is re-read so the screen matches the server. */
  const decide = async (id: string, action: ReviewAction, why?: string): Promise<boolean> => {
    setBusy(true);
    setMessage(null);
    try {
      await reviewDriver(id, action, why);
      setMessage({ kind: "ok", text: `${view === "SUSPENDED" && action === "approve" ? "Reinstated" : VERB[action]} ${id}.` });
      await load();
      return true;
    } catch (err) {
      setMessage({ kind: "error", text: describeReviewError(err) });
      if (profileErrorOf(err) === "invalid_transition") await load();
      return false;
    } finally {
      setBusy(false);
    }
  };

  const startRow = (id: string, action: ReviewAction) => {
    setConfirming({ id, action });
    setReason("");
    setReasonError(null);
    setMessage(null);
  };
  const cancelRow = () => {
    setConfirming(null);
    setReasonError(null);
  };

  const confirmRow = async () => {
    if (!confirming) return;
    const { id, action } = confirming;
    let why: string | undefined;
    if (action === "reject") {
      why = reason.trim();
      if (why === "") return setReasonError("Give a reason for the driver.");
      if (countChars(why) > MAX_REASON) return setReasonError(`At most ${MAX_REASON} characters.`);
    }
    await decide(id, action, why);
    setConfirming(null);
  };

  return (
    <Card className="promotions applications">
      <div className="promotions__head">
        <h2>Driver applications</h2>
        <div className="promotions__actions">
          <select aria-label="Show drivers" value={view} onChange={(e) => setView(e.target.value as View)}>
            <option value="PENDING">Waiting for review</option>
            <option value="SUSPENDED">Suspended</option>
          </select>
          <Button size="sm" variant="ghost" onClick={load}>
            Refresh
          </Button>
        </div>
      </div>

      {message && (
        <p className={message.kind === "error" ? "promotions__error" : "applications__ok"} role={message.kind === "error" ? "alert" : "status"}>
          {message.text}
        </p>
      )}
      {loadError && (
        <p className="promotions__error" role="alert">
          {loadError}
        </p>
      )}

      {rows === null ? (
        !loadError && <p className="ops__empty">Loading applications…</p>
      ) : rows.length === 0 ? (
        <p className="ops__empty">{view === "PENDING" ? "No applications waiting for review." : "No suspended drivers."}</p>
      ) : (
        <div className="promotions__scroll">
          <table className="table promotions__table" aria-label={view === "PENDING" ? "Pending driver applications" : "Suspended drivers"}>
            <thead>
              <tr>
                <th>Driver</th>
                <th>Vehicle</th>
                <th>Plate</th>
                <th>{view === "PENDING" ? "Applied" : "Suspended"}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((a) => {
                const mine = confirming?.id === a.driverId ? confirming : null;
                return (
                  <tr key={a.driverId}>
                    <td>
                      <strong>{a.name}</strong>
                      <div className="promotions__sub">{a.driverId}</div>
                      <div className="promotions__sub">{a.phone ? `${a.phone} (not verified)` : "no phone"}</div>
                    </td>
                    <td>
                      {a.vehicleType === "BIKE" ? "Bike" : a.vehicleType === "CAR" ? "Car" : ""}
                      <div className="promotions__sub">{[a.model, a.color].filter(Boolean).join(" · ")}</div>
                    </td>
                    <td>{a.plate}</td>
                    <td>
                      {when(a.statusAt ?? a.updatedAt)}
                      {view === "SUSPENDED" && a.reason && <div className="promotions__sub">{a.reason}</div>}
                    </td>
                    <td className="applications__actions">
                      {mine === null ? (
                        <>
                          <Button size="sm" variant="secondary" aria-label={`${view === "PENDING" ? "Approve" : "Reinstate"} ${a.driverId}`} disabled={busy} onClick={() => startRow(a.driverId, "approve")}>
                            {view === "PENDING" ? "Approve" : "Reinstate"}
                          </Button>
                          {view === "PENDING" && (
                            <Button size="sm" variant="secondary" aria-label={`Reject ${a.driverId}`} disabled={busy} onClick={() => startRow(a.driverId, "reject")}>
                              Reject
                            </Button>
                          )}
                        </>
                      ) : (
                        <div className="applications__confirm">
                          {mine.action === "reject" && (
                            <TextField
                              label={`Reason for rejecting ${a.driverId}`}
                              value={reason}
                              maxLength={MAX_REASON * 2}
                              autoFocus
                              onChange={(e) => {
                                setReason(e.target.value);
                                setReasonError(null);
                              }}
                              error={reasonError ?? undefined}
                              hint={REASON_NOTE}
                            />
                          )}
                          <div className="promotions__actions">
                            <Button size="sm" variant={mine.action === "approve" ? "primary" : "danger"} loading={busy} onClick={confirmRow}>
                              {`Confirm ${view === "SUSPENDED" ? "reinstate" : mine.action} ${a.driverId}`}
                            </Button>
                            <Button size="sm" variant="ghost" disabled={busy} onClick={cancelRow}>
                              Keep
                            </Button>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {next && (
        <div className="applications__more">
          <Button size="sm" variant="secondary" loading={loadingMore} onClick={loadMore}>
            Load more
          </Button>
        </div>
      )}

      <SuspendForm decide={decide} busy={busy} />
    </Card>
  );
}

/** Suspend an approved driver by id. Two steps: Suspend checks the form and asks for a confirmation. */
function SuspendForm({ decide, busy }: { decide: (id: string, action: ReviewAction, reason?: string) => Promise<boolean>; busy: boolean }) {
  const [driverId, setDriverId] = useState("");
  const [reason, setReason] = useState("");
  const [errors, setErrors] = useState<{ driverId?: string; reason?: string }>({});
  const [confirming, setConfirming] = useState(false);

  const ask = (e: FormEvent) => {
    e.preventDefault();
    const found: typeof errors = {};
    if (driverId.trim() === "") found.driverId = "Enter the driver id.";
    const why = reason.trim();
    if (why === "") found.reason = "Give a reason for the driver.";
    else if (countChars(why) > MAX_REASON) found.reason = `At most ${MAX_REASON} characters.`;
    setErrors(found);
    setConfirming(Object.keys(found).length === 0);
  };

  const confirm = async () => {
    const ok = await decide(driverId.trim(), "suspend", reason.trim());
    setConfirming(false);
    if (ok) {
      setDriverId("");
      setReason("");
    }
  };

  const edit = (set: (v: string) => void) => (e: { target: { value: string } }) => {
    set(e.target.value);
    setConfirming(false);
  };

  return (
    <form className="applications__suspend" onSubmit={ask} noValidate aria-label="Suspend a driver">
      <h3>Suspend a driver</h3>
      <p className="promotions__sub">Takes an approved driver out of service. A trip they are on is finished, and they can't apply again. Reinstate them from the "Suspended" list above.</p>
      <div className="applications__suspend-fields">
        <TextField label="Driver id" value={driverId} autoComplete="off" onChange={edit(setDriverId)} error={errors.driverId} />
        <TextField label="Suspend reason" value={reason} onChange={edit(setReason)} error={errors.reason} hint={REASON_NOTE} />
      </div>
      {confirming ? (
        <div className="promotions__actions">
          <Button type="button" size="sm" variant="danger" loading={busy} onClick={confirm}>
            {`Confirm suspend ${driverId.trim()}`}
          </Button>
          <Button type="button" size="sm" variant="ghost" disabled={busy} onClick={() => setConfirming(false)}>
            Keep
          </Button>
        </div>
      ) : (
        <div className="promotions__actions">
          <Button type="submit" size="sm" variant="secondary" disabled={busy}>
            Suspend
          </Button>
        </div>
      )}
    </form>
  );
}
