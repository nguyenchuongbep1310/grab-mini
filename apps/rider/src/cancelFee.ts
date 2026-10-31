import type { CancellationPolicy } from "@grab-mini/client";

const vnd = (n: number) => `${n.toLocaleString("vi-VN")}đ`;

function duration(seconds: number): string {
  if (seconds % 60 === 0) {
    const m = seconds / 60;
    return m === 1 ? "1 minute" : `${m} minutes`;
  }
  return seconds === 1 ? "1 second" : `${seconds} seconds`;
}

/**
 * The sentence that tells a rider what cancelling will cost once a driver is assigned, or ""
 * when the policy is unknown (not loaded, or Payment Service is down) or cancelling is free.
 * Saying nothing is deliberate when the policy is unknown: a wrong amount would be worse.
 */
export function cancelFeeNote(policy: CancellationPolicy | null): string {
  if (!policy || policy.fee <= 0) return "";
  if (policy.graceSeconds <= 0) return `Cancelling now costs ${vnd(policy.fee)}.`;
  return `Cancel within ${duration(policy.graceSeconds)} of the driver being assigned for free; after that a ${vnd(policy.fee)} fee applies.`;
}
