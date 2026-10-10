import { useEffect, useMemo, useRef, useState } from "react";
import {
  Avatar,
  BottomSheet,
  Button,
  Card,
  MapMarker,
  MapView,
  PriceTag,
  SegmentedControl,
  StarRating,
  StatusBadge,
  SurgeBadge,
  TripStatusTimeline,
  useToast,
  type LatLngTuple,
} from "@grab-mini/ui";
import {
  createOrder,
  newQuoteOf,
  rateTrip,
  signIn,
  tripAction,
  useNotificationSocket,
  waitForTripByOrder,
} from "@grab-mini/client";
import { checkOutcome, findActiveRide } from "./recovery";
import { useQuote } from "./useQuote";
import { useRide, type Phase } from "./useRide";

// Fixed pickup near the seeded drivers (central Ho Chi Minh City).
const PICKUP: LatLngTuple = [10.776, 106.7];
const PICKUP_LATLNG = { lat: PICKUP[0], lng: PICKUP[1] };

const STEPS = [
  { key: "searching", label: "Finding a driver" },
  { key: "assigned", label: "Driver on the way" },
  { key: "in_progress", label: "On your trip" },
  { key: "completed", label: "Arrived" },
];

const VEHICLE_OPTIONS = [
  { value: "CAR" as const, label: "Car", hint: "Up to 4 riders" },
  { value: "BIKE" as const, label: "Bike", hint: "Quick and cheap" },
];

const VEHICLE_NAME = { CAR: "Car", BIKE: "Bike" };

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
// Every request and the WebSocket carry a token for this identity (see @grab-mini/client).
signIn("rider", riderId);

export function App() {
  const toast = useToast();
  const [ride, dispatch] = useRide();
  // Kept through "booking" so a 409 can hand back the new price to confirm.
  const priceQuote = useQuote(PICKUP_LATLNG, ride.dropoff, ride.vehicleType, ride.phase === "idle" || ride.phase === "booking");
  const [cancelling, setCancelling] = useState(false);
  const [stars, setStars] = useState(0);
  const [rated, setRated] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const socket = useNotificationSocket("rider", riderId, (message) => dispatch({ type: "notification", message }));

  // Pick up a ride that was already under way when this page opened.
  useEffect(() => {
    let stale = false;
    findActiveRide(riderId)
      .then((ride) => {
        if (ride && !stale) dispatch({ type: "restored", ride });
      })
      .catch(() => {});
    return () => {
      stale = true;
    };
  }, [dispatch]);

  // While searching, also ask now and then how it turned out. The push normally
  // arrives first; this covers one missed while the page loaded or the socket reconnected.
  useEffect(() => {
    if (ride.phase !== "searching" || !ride.orderId) return;
    const orderId = ride.orderId;
    let stale = false;
    const timer = setInterval(() => {
      checkOutcome(orderId)
        .then((outcome) => {
          if (outcome && !stale) dispatch({ type: "outcome", orderId, outcome });
        })
        .catch(() => {});
    }, 3000);
    return () => {
      stale = true;
      clearInterval(timer);
    };
  }, [ride.phase, ride.orderId, dispatch]);

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
      const { orderId, price, surgeMultiplier } = await createOrder(
        riderId,
        PICKUP_LATLNG,
        ride.dropoff,
        ride.vehicleType,
        priceQuote.quote?.quoteId,
      );
      priceQuote.acknowledge();
      dispatch({ type: "bookSucceeded", orderId, price, surgeMultiplier });
    } catch (err) {
      dispatch({ type: "bookFailed" });
      // The price moved since the rider last saw it: show the new one and let them confirm.
      const fresh = newQuoteOf(err);
      if (fresh) {
        priceQuote.applyNewQuote(fresh);
        toast.show("The price changed. Please confirm the new price.", "info");
        return;
      }
      toast.show(err instanceof Error ? err.message : "Couldn't book the ride", "error");
    }
  };

  const submitRating = async () => {
    if (!tripId || stars === 0) return;
    setSubmitting(true);
    try {
      await rateTrip(tripId, riderId, stars);
      setRated(true);
      toast.show("Thanks for your feedback!", "success");
    } catch (err) {
      toast.show(err instanceof Error ? err.message : "Couldn't send your rating", "error");
    } finally {
      setSubmitting(false);
    }
  };

  const newRide = () => {
    setStars(0);
    setRated(false);
    dispatch({ type: "reset" });
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
        bottomInset={phase === "idle" ? 270 : 320}
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
            <SegmentedControl
              label="Vehicle type"
              value={ride.vehicleType}
              onChange={(vehicleType) => dispatch({ type: "setVehicle", vehicleType })}
              options={VEHICLE_OPTIONS}
            />
            {ride.dropoff && priceQuote.quote && (
              <div className="price-row">
                <PriceTag size="lg" amount={priceQuote.quote.price} currency={priceQuote.quote.currency} />
                <SurgeBadge multiplier={priceQuote.quote.multiplier} />
              </div>
            )}
            {ride.dropoff && !priceQuote.quote && priceQuote.status === "error" && (
              <p className="sheet-sub">Price unavailable right now. It will be set when you book.</p>
            )}
            <Button size="lg" block disabled={!ride.dropoff || (priceQuote.status === "loading" && !priceQuote.quote)} onClick={book}>
              {priceQuote.changed ? "Confirm new price" : "Book ride"}
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
                    <div className="driver-row__meta">Your driver · {VEHICLE_NAME[ride.vehicleType]}</div>
                  </div>
                  <StatusBadge className="driver-row__badge" status={BADGE_STATUS[phase]} />
                </div>
              </Card>
            )}

            {!inRide && <StatusBadge status={BADGE_STATUS[phase]} />}

            {ride.price !== null && (
              <div className="price-row">
                <PriceTag amount={ride.price} />
                <SurgeBadge multiplier={ride.surgeMultiplier} />
              </div>
            )}

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
            {phase === "completed" && (
              <Card padding="sm">
                <div className="rating-box">
                  <div className="driver-row__name">{rated ? "Your rating" : `How was your ride with ${driverId}?`}</div>
                  <StarRating
                    value={stars}
                    size="lg"
                    label="Rate your driver"
                    onChange={rated ? undefined : setStars}
                    disabled={submitting}
                  />
                  {!rated && (
                    <Button block loading={submitting} disabled={stars === 0 || !tripId} onClick={submitRating}>
                      Submit rating
                    </Button>
                  )}
                </div>
              </Card>
            )}
            {finished && (
              <Button block variant={phase === "completed" && !rated ? "secondary" : "primary"} onClick={newRide}>
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
