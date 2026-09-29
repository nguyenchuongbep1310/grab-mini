import { useMemo } from "react";
import { Marker } from "react-leaflet";
import { divIcon } from "leaflet";
import type { LatLngTuple } from "./MapView";

export interface MapMarkerProps {
  position: LatLngTuple;
  variant: "pickup" | "dropoff" | "driver";
}

const LABELS = { pickup: "A", dropoff: "B", driver: "" } as const;

export function MapMarker({ position, variant }: MapMarkerProps) {
  const icon = useMemo(
    () =>
      divIcon({
        className: `gm-marker gm-marker--${variant}`,
        html:
          variant === "driver"
            ? '<div class="gm-marker__dot"></div>'
            : `<div class="gm-marker__pin"><span>${LABELS[variant]}</span></div>`,
        iconSize: variant === "driver" ? [22, 22] : [28, 28],
        iconAnchor: variant === "driver" ? [11, 11] : [14, 28],
      }),
    [variant],
  );
  return <Marker position={position} icon={icon} />;
}
