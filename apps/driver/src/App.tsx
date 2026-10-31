import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Avatar,
  BottomSheet,
  Button,
  CancelDialog,
  Card,
  ChatButton,
  ChatSheet,
  FormSheet,
  MapMarker,
  MapView,
  StarRating,
  StatusBadge,
  TripStatusTimeline,
  useToast,
  type LatLngTuple,
} from "@grab-mini/ui";
import {
  getActiveTrip,
  getDriverRating,
  waitForOrder,
  signIn,
  tripAction,
  useChat,
  useNotificationSocket,
  waitForTripByOrder,
  type DriverRating,
  type TripAction,
} from "@grab-mini/client";
import type { CancelledBy } from "@grab-mini/events";
import { DriverForm } from "./DriverForm";
import { EarningsLine, TripEarnings } from "./Earnings";
import { StatusScreen } from "./StatusScreen";
import { useDriverProfile } from "./useDriverProfile";
import { useDriverTrip, type Phase } from "./useDriverTrip";

// Where the map sits until the first location arrives (central Ho Chi Minh City).
const DEFAULT_CENTER: LatLngTuple = [10.776, 106.7];

const STEPS = [
  { key: "assigned", label: "Head to pickup" },
  { key: "in_progress", label: "Trip in progress" },
  { key: "completed", label: "Drop-off complete" },
];

const BADGE_STATUS: Record<Phase, string> = {
  idle: "AVAILABLE",
  assigned: "ASSIGNED",
  in_progress: "IN_PROGRESS",
  completed: "COMPLETED",
  cancelled: "CANCELLED",
};

// The shared badge words statuses from the rider's point of view; these read right for the driver.
const BADGE_LABEL: Partial<Record<Phase, string>> = {
  assigned: "Heading to pickup",
  in_progress: "On trip",
};

const CANCEL_REASONS = ["Rider isn't at the pickup point", "Rider asked me to cancel", "Vehicle problem", "Emergency"];

/** What the driver reads once a trip is cancelled, depending on who cancelled it. */
function cancelledCopy(by: CancelledBy, reason: string) {
  const why = reason ? ` Reason: ${reason}.` : "";
  return by === "DRIVER"
    ? { title: "You cancelled the trip", sub: `You're free to take the next request.${why}` }
    : { title: "Rider cancelled the trip", sub: `The rider cancelled this trip. You're free to take the next request.${why}` };
}

const driverId = new URLSearchParams(location.search).get("driver") ?? "driver-1";
// Every request and the WebSocket carry a token for this identity (see @grab-mini/client).
signIn("driver", driverId);

export function App() {
  const toast = useToast();
  const [state, dispatch] = useDriverTrip();
  // Who may drive: only an APPROVED driver sees "You're online" and trips (see onboarding.ts).
  const profile = useDriverProfile(driverId);
  const [editingProfile, setEditingProfile] = useState(false);
  const [pending, setPending] = useState<TripAction | null>(null);
  const [rating, setRating] = useState<DriverRating | null>(null);
  const [confirmingCancel, setConfirmingCancel] = useState(false);
  // Bumped when a rating arrives: its tip is a payment that settles a moment later.
  const [ratings, setRatings] = useState(0);

  const refreshRating = useCallback(() => {
    getDriverRating(driverId).then(setRating).catch(() => {});
  }, []);
  useEffect(refreshRating, [refreshRating]);

  // Pick up a trip that was already under way when this page opened.
  useEffect(() => {
    let stale = false;
    getActiveTrip(driverId)
      .then((trip) => {
        if (trip && !stale) dispatch({ type: "restored", tripId: trip.id, orderId: trip.orderId, status: trip.status });
      })
      .catch(() => {});
    return () => {
      stale = true;
    };
  }, [dispatch]);

  // Chat pushes go to the chat hook (created below, once the trip is known), never to the trip reducer.
  const chatPush = useRef<(message: unknown) => boolean>(() => false);
  const socket = useNotificationSocket("driver", driverId, (message) => {
    if (chatPush.current(message)) return;
    if (message.type === "trip.rated") {
      const tip = message.payload.tipAmount ?? 0;
      toast.show(
        `${message.payload.stars}-star rating from ${message.payload.riderId}${tip > 0 ? `, tip ${tip.toLocaleString("vi-VN")}đ` : ""}`,
        "success",
      );
      // Rating Service updates its totals asynchronously, so give it a moment.
      setTimeout(refreshRating, 800);
      setRatings((n) => n + 1);
      return;
    }
    dispatch({ type: "notification", message, driverId });
  });

  // order.matched carries neither the tripId nor the route, so fetch both.
  const { phase, orderId, tripId, order, cancelledBy, cancelReason } = state;
  useEffect(() => {
    if (phase === "idle" || !orderId) return;
    let stale = false;
    if (!tripId) {
      waitForTripByOrder(orderId)
        .then((trip) => !stale && dispatch({ type: "tripLoaded", tripId: trip.id }))
        .catch(() => !stale && toast.show("Couldn't load the trip", "error"));
    }
    if (!order) {
      waitForOrder(orderId)
        .then((o) => !stale && dispatch({ type: "orderLoaded", order: o }))
        .catch(() => !stale && toast.show("Couldn't load the route", "error"));
    }
    return () => {
      stale = true;
    };
  }, [phase, orderId, tripId, order, dispatch, toast]);

  const chat = useChat(tripId, {
    me: driverId,
    enabled: !!tripId && phase !== "idle",
    socketState: socket,
  });
  useEffect(() => {
    chatPush.current = chat.receive;
  }, [chat.receive]);

  const previousPhase = useRef<Phase>(phase);
  useEffect(() => {
    if (previousPhase.current === phase) return;
    previousPhase.current = phase;
    if (phase === "assigned") toast.show("New trip assigned to you", "success");
    if (phase === "cancelled") toast.show(cancelledCopy(cancelledBy, "").title, "info");
    if (phase === "completed") toast.show("Trip completed. Nice work!", "success");
  }, [phase, cancelledBy, toast]);

  const act = async (action: TripAction, reason?: string) => {
    if (!tripId) return;
    setPending(action);
    try {
      await tripAction(tripId, action, reason);
    } catch (err) {
      toast.show(err instanceof Error ? err.message : "That didn't work", "error");
    } finally {
      setPending(null);
      setConfirmingCancel(false);
    }
  };

  const pickup: LatLngTuple | null = order ? [order.pickup.lat, order.pickup.lng] : null;
  const dropoff: LatLngTuple | null = order ? [order.dropoff.lat, order.dropoff.lng] : null;
  const pos: LatLngTuple | null = state.pos ? [state.pos.lat, state.pos.lng] : null;
  const anchor: LatLngTuple | null = state.anchor ? [state.anchor.lat, state.anchor.lng] : null;

  const fitPoints = useMemo<LatLngTuple[]>(() => {
    if (phase === "idle" || phase === "completed" || phase === "cancelled") return [pos ?? DEFAULT_CENTER];
    return [...(anchor ? [anchor] : []), ...(pickup ? [pickup] : []), ...(dropoff ? [dropoff] : [])];
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [phase, anchor?.join(","), pickup?.join(","), dropoff?.join(","), phase === "idle" && pos ? "has-pos" : ""]);

  const working = phase === "assigned" || phase === "in_progress";
  const { gate } = profile;
  const approvedProfile = gate.kind === "app" ? gate.profile : null;
  // The gate replaces the idle screen. A trip already under way (or just finished) stays visible even if the
  // driver was suspended or sent back to review meanwhile: Matching lets them finish it, and hiding it would
  // strand a rider.
  const gated = gate.kind !== "app";
  const showGate = gate.kind !== "app" && phase === "idle";
  // While the trip is on, and afterwards for as long as the trip is still shown and there is something to re-read.
  const showChat = !!tripId && (working || ((phase === "completed" || phase === "cancelled") && chat.messages.length > 0));

  return (
    <div className="app gm-root">
      <MapView className="app__map" center={pos ?? DEFAULT_CENTER} zoom={15} fitTo={fitPoints} bottomInset={working ? 300 : 180}>
        {pickup && working && <MapMarker position={pickup} variant="pickup" />}
        {dropoff && working && <MapMarker position={dropoff} variant="dropoff" />}
        {pos && <MapMarker position={pos} variant="driver" />}
      </MapView>

      <header className="app__bar">
        <span className="app__brand">grab-mini driver</span>
        <span className="app__spacer" />
        <span className="app__conn" data-state={socket}>
          {socket === "open" ? (gated ? "Connected" : "Online") : socket === "connecting" ? "Connecting" : "Offline"}
        </span>
        {approvedProfile ? (
          <button type="button" className="app__avatar" aria-label="Your driver profile" onClick={() => setEditingProfile(true)}>
            <Avatar name={approvedProfile.name || driverId} size="sm" />
          </button>
        ) : (
          <Avatar name={driverId} size="sm" />
        )}
      </header>

      <BottomSheet>
        <div className="sheet-stack">
          {showGate && (
            <StatusScreen
              key={gate.kind}
              gate={gate}
              driverId={driverId}
              onRefresh={profile.refresh}
              onRetry={profile.retry}
              onDone={profile.accept}
            />
          )}

          {phase === "idle" && !gated && (
            <>
              {gate.kind === "app" && gate.profile === null && (
                <p className="onboarding__note" role="status">
                  Couldn't check your driver profile right now. If you are approved you will still get trips.{" "}
                  <button type="button" className="onboarding__link" onClick={profile.retry}>
                    Check again
                  </button>
                </p>
              )}
              <div>
                <h1 className="sheet-title">You're online</h1>
                <p className="sheet-sub">
                  {pos
                    ? "Waiting for the next trip request."
                    : "Waiting for your location. Run driver-app-sim to broadcast it."}
                </p>
              </div>
              <StatusBadge status="AVAILABLE" />
              {rating && (
                <div className="rating-line">
                  <StarRating value={rating.average} size="sm" label="Your rating" />
                  <span>
                    {rating.count > 0 ? `${rating.average.toFixed(2)} · ${rating.count} ${rating.count === 1 ? "rating" : "ratings"}` : "No ratings yet"}
                  </span>
                </div>
              )}
              <EarningsLine driverId={driverId} refreshKey={ratings} />
            </>
          )}

          {phase !== "idle" && (
            <>
              {gated && gate.kind !== "checking" && (
                <p className="onboarding__note" role="status">
                  Your driver account isn't active, so you won't be offered new trips after this one.
                </p>
              )}
              <div>
                <h1 className="sheet-title">{phase === "cancelled" ? cancelledCopy(cancelledBy, cancelReason).title : TITLES[phase]}</h1>
                <p className="sheet-sub">{phase === "cancelled" ? cancelledCopy(cancelledBy, cancelReason).sub : SUBTITLES[phase]}</p>
              </div>

              <Card padding="sm">
                <dl className="trip-facts">
                  <dt>Status</dt>
                  <dd>
                    <StatusBadge status={BADGE_STATUS[phase]} label={BADGE_LABEL[phase]} />
                  </dd>
                  <dt>Order</dt>
                  <dd>{orderId?.slice(0, 8)}…</dd>
                  <dt>Rider</dt>
                  <dd>{order?.riderId ?? "…"}</dd>
                  <dt>Vehicle</dt>
                  <dd>{order ? (order.vehicleType === "BIKE" ? "Bike" : "Car") : "…"}</dd>
                </dl>
              </Card>

              <TripStatusTimeline
                steps={STEPS}
                currentKey={phase === "cancelled" ? "assigned" : phase}
                failed={phase === "cancelled"}
              />

              {showChat && <ChatButton unread={chat.unread} onClick={chat.openSheet} />}

              {phase === "assigned" && (
                <div className="btn-row">
                  <Button variant="secondary" loading={pending === "cancel"} disabled={!tripId || !!pending} onClick={() => setConfirmingCancel(true)}>
                    Cancel
                  </Button>
                  <Button loading={pending === "pickup"} disabled={!tripId || !!pending} onClick={() => act("pickup")}>
                    Confirm pickup
                  </Button>
                </div>
              )}
              {phase === "in_progress" && (
                <div className="btn-row">
                  <Button variant="secondary" loading={pending === "cancel"} disabled={!tripId || !!pending} onClick={() => setConfirmingCancel(true)}>
                    Cancel
                  </Button>
                  <Button loading={pending === "complete"} disabled={!tripId || !!pending} onClick={() => act("complete")}>
                    Complete trip
                  </Button>
                </div>
              )}
              {(phase === "completed" || phase === "cancelled") && <TripEarnings tripId={tripId} refreshKey={ratings} />}
              {(phase === "completed" || phase === "cancelled") && (
                <Button block onClick={() => dispatch({ type: "reset" })}>
                  Back online
                </Button>
              )}
            </>
          )}
        </div>
      </BottomSheet>

      <FormSheet open={editingProfile && !!approvedProfile} title="Your driver profile" onClose={() => setEditingProfile(false)}>
        <DriverForm
          driverId={driverId}
          initial={approvedProfile}
          mode="edit"
          submitLabel="Save changes"
          onDone={(saved) => {
            profile.accept(saved);
            setEditingProfile(false);
            toast.show(saved.status === "APPROVED" ? "Profile saved" : "Sent for review. No trips until an admin approves it.", saved.status === "APPROVED" ? "success" : "info");
          }}
        />
      </FormSheet>

      <ChatSheet
        open={chat.sheetOpen && showChat}
        title={`Chat with ${order?.riderId ?? "your rider"}`}
        messages={chat.messages}
        loading={chat.loading}
        closed={chat.closed}
        sendDisabled={chat.sendBlocked}
        notice={chat.notice}
        onSend={chat.send}
        onRetry={chat.retry}
        onClose={chat.closeSheet}
      />

      <CancelDialog
        open={confirmingCancel && working}
        title="Cancel this trip?"
        description="The rider will be told, and you'll be free for the next request."
        reasons={CANCEL_REASONS}
        confirmLabel="Cancel trip"
        dismissLabel="Keep trip"
        loading={pending === "cancel"}
        onConfirm={(reason) => act("cancel", reason)}
        onDismiss={() => setConfirmingCancel(false)}
      />
    </div>
  );
}

const TITLES: Record<Phase, string> = {
  idle: "",
  assigned: "New trip",
  in_progress: "Trip in progress",
  completed: "Trip completed",
  cancelled: "Trip cancelled",
};

const SUBTITLES: Record<Phase, string> = {
  idle: "",
  assigned: "Head to the pickup point, then confirm when the rider is in.",
  in_progress: "Drive to the drop-off point and complete the trip on arrival.",
  completed: "You're free to take the next request.",
  cancelled: "This trip was cancelled. You're free to take the next request.",
};
