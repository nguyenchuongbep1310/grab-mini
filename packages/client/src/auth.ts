// Who this browser tab is signed in as, and the token that proves it.
//
// In development the tokens come from auth-service, a stand-in identity
// provider that signs a token for whoever asks (see services/auth-service).
// Everything downstream (the REST calls and the WebSocket) only ever sees a
// token, so swapping in a real login means changing this file.

export type Role = "rider" | "driver";

const AUTH_API = "/auth-api";

/** Refresh when fewer than this many milliseconds remain, so a request never goes out with a token about to expire. */
const REFRESH_MARGIN_MS = 60_000;

interface Session {
  role: Role;
  id: string;
  token?: string;
  expiresAt?: number;
  /** A token request already under way, shared by concurrent callers. */
  pending?: Promise<string>;
}

let session: Session | null = null;

/** Sign this tab in as a rider or driver. Call once, before any request. */
export function signIn(role: Role, id: string): void {
  session = { role, id };
}

export function isSignedIn(): boolean {
  return session !== null;
}

/** Drop the cached token, e.g. after the server said it was no longer valid. */
export function forgetToken(): void {
  if (session) {
    session.token = undefined;
    session.expiresAt = undefined;
  }
}

/** A valid token for the signed-in user, fetching or refreshing it if needed. */
export async function getToken(): Promise<string> {
  const s = session;
  if (!s) throw new Error("Not signed in: call signIn(role, id) first");

  if (s.token && s.expiresAt && s.expiresAt - Date.now() > REFRESH_MARGIN_MS) return s.token;
  if (s.pending) return s.pending;

  s.pending = (async () => {
    try {
      const res = await fetch(`${AUTH_API}/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ role: s.role, id: s.id }),
      });
      if (!res.ok) throw new Error(`Sign-in failed (${res.status})`);
      const body = (await res.json()) as { token: string; expiresAt: string };
      s.token = body.token;
      s.expiresAt = Date.parse(body.expiresAt);
      return body.token;
    } finally {
      s.pending = undefined;
    }
  })();
  return s.pending;
}
