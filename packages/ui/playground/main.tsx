import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Avatar,
  BottomSheet,
  Button,
  Card,
  MapMarker,
  MapView,
  SegmentedControl,
  PriceTag,
  Stat,
  StarRating,
  StatusBadge,
  SurgeBadge,
  ToastProvider,
  TripStatusTimeline,
  useToast,
  type LatLngTuple,
} from "../src";

const STEPS = [
  { key: "searching", label: "Finding a driver" },
  { key: "assigned", label: "Driver on the way", hint: "driver-1 · arriving soon" },
  { key: "in_progress", label: "On trip" },
  { key: "completed", label: "Arrived" },
];

const PICKUP: LatLngTuple = [10.776, 106.7];
const DROPOFF: LatLngTuple = [10.79, 106.71];

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section style={{ marginBottom: 32 }}>
      <h2 style={{ fontSize: 14, textTransform: "uppercase", letterSpacing: 0.6, color: "var(--gm-text-muted)" }}>{title}</h2>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "flex-start" }}>{children}</div>
    </section>
  );
}

function Gallery() {
  const toast = useToast();
  const [step, setStep] = useState("assigned");
  const [dark, setDark] = useState(false);
  const [stars, setStars] = useState(0);
  const [vehicle, setVehicle] = useState<"CAR" | "BIKE">("CAR");

  useEffect(() => {
    document.documentElement.dataset.theme = dark ? "dark" : "light";
  }, [dark]);

  return (
    <div className="gm-root" style={{ minHeight: "100vh", padding: 24 }}>
      <div style={{ maxWidth: 760, margin: "0 auto" }}>
        <h1 style={{ margin: "0 0 4px" }}>grab-mini UI</h1>
        <p style={{ color: "var(--gm-text-muted)", marginTop: 0 }}>Design system playground</p>
        <Button size="sm" variant="secondary" onClick={() => setDark((d) => !d)} style={{ marginBottom: 24 }}>
          Theme: {dark ? "dark" : "light"}
        </Button>

        <Section title="Button">
          <Button>Primary</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="danger">Danger</Button>
          <Button loading>Loading</Button>
          <Button disabled>Disabled</Button>
          <Button size="sm">Small</Button>
          <Button size="lg">Large</Button>
        </Section>

        <Section title="SegmentedControl">
          <div style={{ width: 320 }}>
            <SegmentedControl
              label="Vehicle type"
              value={vehicle}
              onChange={setVehicle}
              options={[
                { value: "CAR", label: "Car", hint: "4 seats" },
                { value: "BIKE", label: "Bike", hint: "Faster" },
              ]}
            />
          </div>
        </Section>

        <Section title="Stat">
          <Card style={{ display: "flex", gap: 32 }}>
            <Stat label="Orders" value="128" hint="last hour" />
            <Stat label="Match rate" value="94%" tone="success" hint="121 of 128" />
            <Stat label="No driver" value="7" tone="danger" />
          </Card>
        </Section>

        <Section title="StarRating">
          <Card>
            <div style={{ marginBottom: 8, fontSize: 13, color: "var(--gm-text-muted)" }}>Rate your driver</div>
            <StarRating value={stars} onChange={setStars} size="lg" label="Rate your driver" />
          </Card>
          <Card>
            <div style={{ marginBottom: 8, fontSize: 13, color: "var(--gm-text-muted)" }}>Average, read-only</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <StarRating value={4.7} label="Driver rating" />
              <StarRating value={3.2} size="sm" label="Driver rating" />
              <StarRating value={0} size="sm" label="Driver rating" />
            </div>
          </Card>
        </Section>

        <Section title="PriceTag and SurgeBadge">
          <PriceTag amount={62000} />
          <PriceTag size="lg" amount={87000} was={62000} />
          <SurgeBadge multiplier={1.4} />
          <SurgeBadge multiplier={1} />
        </Section>

        <Section title="StatusBadge">
          {["PENDING", "ASSIGNED", "IN_PROGRESS", "COMPLETED", "CANCELLED", "NO_DRIVER", "AVAILABLE"].map((s) => (
            <StatusBadge key={s} status={s} />
          ))}
        </Section>

        <Section title="Avatar">
          <Avatar name="driver-1" size="sm" />
          <Avatar name="Chuong Nguyen" />
          <Avatar name="rider-7" size="lg" />
        </Section>

        <Section title="Card">
          <Card style={{ width: 320 }}>
            <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
              <Avatar name="driver-3" />
              <div>
                <div style={{ fontWeight: 600 }}>driver-3</div>
                <div style={{ fontSize: 13, color: "var(--gm-text-muted)" }}>2 min away</div>
              </div>
              <StatusBadge status="ASSIGNED" />
            </div>
          </Card>
          <Card elevated style={{ width: 200 }}>Elevated card</Card>
        </Section>

        <Section title="Toast">
          <Button variant="secondary" onClick={() => toast.show("Driver found", "success")}>Success</Button>
          <Button variant="secondary" onClick={() => toast.show("Trip was cancelled", "error")}>Error</Button>
          <Button variant="secondary" onClick={() => toast.show("Connecting…", "info")}>Info</Button>
        </Section>

        <Section title="TripStatusTimeline">
          <Card style={{ width: 300 }}>
            <TripStatusTimeline steps={STEPS} currentKey={step} />
          </Card>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {STEPS.map((s) => (
              <Button key={s.key} size="sm" variant={step === s.key ? "primary" : "secondary"} onClick={() => setStep(s.key)}>
                {s.key}
              </Button>
            ))}
          </div>
          <Card style={{ width: 300 }}>
            <TripStatusTimeline steps={STEPS} currentKey="assigned" failed />
          </Card>
        </Section>

        <Section title="MapView + MapMarker + BottomSheet">
          <div style={{ position: "relative", width: "100%", height: 420, borderRadius: 20, overflow: "hidden" }}>
            <MapView center={PICKUP} fitTo={[PICKUP, DROPOFF, [10.7825, 106.705]]} bottomInset={110}>
              <MapMarker position={PICKUP} variant="pickup" />
              <MapMarker position={DROPOFF} variant="dropoff" />
              <MapMarker position={[10.7825, 106.705]} variant="driver" />
            </MapView>
            <BottomSheet defaultCollapsed>
              <div style={{ fontWeight: 600, marginBottom: 8 }}>Your driver is on the way</div>
              <TripStatusTimeline steps={STEPS} currentKey={step} />
            </BottomSheet>
          </div>
        </Section>
      </div>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ToastProvider>
      <Gallery />
    </ToastProvider>
  </StrictMode>,
);
