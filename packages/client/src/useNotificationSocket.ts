import { useEffect, useRef, useState } from "react";
import type { NotificationMessage } from "@grab-mini/events";

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

    const connect = () => {
      setState("connecting");
      const scheme = location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${scheme}://${location.host}/ws/${role}/${encodeURIComponent(id)}`);

      socket.onopen = () => {
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
        setState("closed");
        retryTimer = setTimeout(connect, Math.min(1000 * 2 ** attempt++, 10_000));
      };
    };

    connect();
    return () => {
      disposed = true;
      clearTimeout(retryTimer);
      socket?.close();
    };
  }, [role, id]);

  return state;
}
