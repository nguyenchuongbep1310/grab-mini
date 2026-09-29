import { useEffect, useMemo, useRef, useState } from "react";
import {
  Avatar,
  BottomSheet,
  Button,
  Card,
  MapMarker,
  MapView,
  StatusBadge,
  TripStatusTimeline,
  useToast,
  type LatLngTuple,
} from "@grab-mini/ui";
import { createOrder, tripAction, useNotificationSocket, waitForTripByOrder } from "@grab-mini/client";
import { useRide, type Phase } from "./useRide";

// Fixed pickup near the seeded drivers (central Ho Chi Minh City).
const PICKUP: LatLngTuple = [10.776, 106.7];

const STEPS = [
  { key: "searching", label: "Finding a driver" },
  { key: "assigned", label: "Driver on the way" },
  { key: "in_progress", label: "On your trip" },
  { key: "completed", label: "Arrived" },
];

const BADGE_STATUS: Record<Phase, string> = {
  idle: "",
  booking: "PENDING",
  searching: "PENDING",
  assigned: "ASSIGNED",
  in_progress: "IN_PROGRESS",
  completed: "COMPLETED",
  cancelled: "CANCELLED",
  no_driver: "NO_DRIVER",
};

const riderId = new URLSearchParams(location.search).get("rider") ?? "rider-1";

export function App() {
  const toast = useToast();
  const [ride, dispatch] = useRide();
  const [cancelling, setCancelling] = useState(false);
  const socket = useNotificationSocket("rider", riderId, (message) => dispatch({ type: "notification", message }));

  // order.matched carries no tripId; look the trip up once the driver is assigned.
  const { phase, orderId, tripId, driverId } = ride;
  useEffect(() => {
    if (phase !== "assigned" || !orderId || tripId) return;
    let cancelled = false;
    waitForTripByOrder(orderId)
      .then((trip) => !cancelled && dispatch({ type: "tripLoaded", tripId: trip.id }))
      .catch(() => !cancelled && toast.show("Couldn't load your trip details", "error"));
    return () => {
      cancelled = true;
    };
  }, [phase, orderId, tripId, dispatch, toast]);

  const previousPhase = useRef<Phase>(phase);
  useEffect(() => {
    if (previousPhase.current === phase) return;
    previousPhase.current = phase;
    if (phase === "assigned") toast.show(`Driver found: ${driverId}`, "success");
    if (phase === "no_driver") toast.show("No drivers available nearby", "error");
    if (phase === "cancelled") toast.show("This trip was cancelled", "info");
    if (phase === "completed") toast.show("You've arrived. Thanks for riding!", "success");
  }, [phase, driverId, toast]);

  const book = async () => {
    if (!ride.dropoff) return;
    dispatch({ type: "bookStarted" });
    try {
      const { orderId } = await createOrder(riderId, { lat: PICKUP[0], lng: PICKUP[1] }, ride.dropoff);
      dispatch({ type: "bookSucceeded", orderId });
    } catch (err) {
      dispatch({ type: "bookFailed" });
      toast.show(err instanceof Error ? err.message : "Couldn't book the ride", "error");
    }
  };

  const cancel = async () => {
    if (!tripId) return;
    setCancelling(true);
    try {
      await tripAction(tripId, "cancel");
    } catch (err) {
      toast.show(err instanceof Error ? err.message : "Couldn't cancel the trip", "error");
    } finally {
      setCancelling(false);
    }
  };

  const dropoff: LatLngTuple | null = ride.dropoff ? [ride.dropoff.lat, ride.dropoff.lng] : null;
  const driverPos: LatLngTuple | null = ride.driverPos ? [ride.driverPos.lat, ride.driverPos.lng] : null;
  const driverAnchor: LatLngTuple | null = ride.driverAnchor ? [ride.driverAnchor.lat, ride.driverAnchor.lng] : null;

  const fitPoints = useMemo(
    () => [PICKUP, ...(dropoff ? [dropoff] : []), ...(driverAnchor ? [driverAnchor] : [])],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [dropoff?.join(","), driverAnchor?.join(",")],
  );

  const finished = phase === "completed" || phase === "cancelled" || phase === "no_driver";
  const inRide = phase === "assigned" || phase === "in_progress";

  return (
    <div className="app gm-root">
      <MapView
        className="app__map"
        center={PICKUP}
        zoom={15}
        fitTo={fitPoints}
        bottomInset={phase === "idle" ? 200 : 320}
        onClick={phase === "idle" ? (p) => dispatch({ type: "setDropoff", dropoff: { lat: p[0], lng: p[1] } }) : undefined}
      >
        <MapMarker position={PICKUP} variant="pickup" />
        {dropoff && <MapMarker position={dropoff} variant="dropoff" />}
        {driverPos && inRide && <MapMarker position={driverPos} variant="driver" />}
      </MapView>

      <header className="app__bar">
        <span className="app__brand">grab-mini</span>
        <span className="app__spacer" />
        <span className="app__conn" data-state={socket}>
          {socket === "open" ? "Live" : socket === "connecting" ? "Connecting" : "Offline"}
        </span>
        <Avatar name={riderId} size="sm" />
      </header>

      <BottomSheet>
        {phase === "idle" && (
          <div className="sheet-stack">
            <div>
              <h1 className="sheet-title">Where to?</h1>
              <p className="sheet-sub">
                {ride.dropoff
                  ? "Destination set. You can tap the map again to change it."
                  : "Tap the map to drop a destination pin."}
              </p>
            </div>
            <Button size="lg" block disabled={!ride.dropoff} onClick={book}>
              Book ride
            </Button>
          </div>
        )}

        {phase !== "idle" && (
          <div className="sheet-stack">
            <div>
              <h1 className="sheet-title">{TITLES[phase]}</h1>
              <p className="sheet-sub">{SUBTITLES[phase]}</p>
            </div>

            {inRide && driverId && (
              <Card padding="sm">
                <div className="driver-row">
                  <Avatar name={driverId} />
                  <div>
                    <div className="driver-row__name">{driverId}</div>
                    <div className="driver-row__meta">Your driver</div>
                  </div>
                  <StatusBadge className="driver-row__badge" status={BADGE_STATUS[phase]} />
                </div>
              </Card>
            )}

            {!inRide && <StatusBadge status={BADGE_STATUS[phase]} />}

            <TripStatusTimeline
              steps={STEPS}
              currentKey={ride.reached}
              failed={phase === "cancelled" || phase === "no_driver"}
            />

            {phase === "assigned" && (
              <Button variant="secondary" block loading={cancelling} disabled={!tripId} onClick={cancel}>
                Cancel trip
              </Button>
            )}
            {finished && (
              <Button block onClick={() => dispatch({ type: "reset" })}>
                New ride
              </Button>
            )}
          </div>
        )}
      </BottomSheet>
    </div>
  );
}

const TITLES: Record<Phase, string> = {
  idle: "",
  booking: "Booking your ride…",
  searching: "Looking for a driver",
  assigned: "Your driver is on the way",
  in_progress: "Enjoy your ride",
  completed: "You've arrived",
  cancelled: "Trip cancelled",
  no_driver: "No drivers nearby",
};

const SUBTITLES: Record<Phase, string> = {
  idle: "",
  booking: "Hang tight, sending your request.",
  searching: "We're matching you with the closest driver.",
  assigned: "Watch them approach on the map.",
  in_progress: "Sit back. We'll let you know when you arrive.",
  completed: "Thanks for riding with grab-mini.",
  cancelled: "This trip was cancelled. You can book another ride.",
  no_driver: "All nearby drivers are busy right now. Try again in a moment.",
};
