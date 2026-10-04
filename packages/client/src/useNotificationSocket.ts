import { useEffect, useRef, useState } from "react";
import type { NotificationMessage } from "@grab-mini/events";
import { forgetToken, getToken } from "./auth";

export type SocketState = "connecting" | "open" | "closed";

/**
 * Connects to Notification Service as a rider or driver and keeps the
 * connection alive (reconnecting with a capped backoff) for as long as
 * the component is mounted.
 */
export function useNotificationSocket(
  role: "rider" | "driver",
  id: string,
  onMessage: (message: NotificationMessage) => void,
): SocketState {
  const [state, setState] = useState<SocketState>("connecting");

  // Always call the latest handler without re-opening the socket when it changes.
  const handler = useRef(onMessage);
  useEffect(() => {
    handler.current = onMessage;
  });

  useEffect(() => {
    let socket: WebSocket | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    let disposed = false;

    const connect = async () => {
      setState("connecting");
      let token: string;
      try {
        token = await getToken();
      } catch {
        // auth-service unreachable: treat it like a dropped connection and try again.
        if (!disposed) retryTimer = setTimeout(() => void connect(), Math.min(1000 * 2 ** attempt++, 10_000));
        return;
      }
      if (disposed) return;
      const scheme = location.protocol === "https:" ? "wss" : "ws";
      // A browser can't set an Authorization header on a WebSocket; the token
      // travels as a subprotocol ("bearer", <token>), which unlike a query
      // string doesn't end up in access logs or the address bar.
      socket = new WebSocket(`${scheme}://${location.host}/ws/${role}/${encodeURIComponent(id)}`, ["bearer", token]);

      let opened = false;
      socket.onopen = () => {
        opened = true;
        attempt = 0;
        setState("open");
      };
      socket.onmessage = (event) => {
        try {
          handler.current(JSON.parse(event.data) as NotificationMessage);
        } catch (err) {
          console.error("Ignoring malformed notification", err);
        }
      };
      socket.onclose = () => {
        if (disposed) return;
        // A refused handshake looks like any failed connection to the page;
        // fetch a fresh token next time in case that was an expired one.
        if (!opened) forgetToken();
        setState("closed");
        retryTimer = setTimeout(() => void connect(), Math.min(1000 * 2 ** attempt++, 10_000));
      };
    };

    void connect();
    return () => {
      disposed = true;
      clearTimeout(retryTimer);
      socket?.close();
    };
  }, [role, id]);

  return state;
}
