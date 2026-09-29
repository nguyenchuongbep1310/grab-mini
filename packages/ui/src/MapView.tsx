import { useEffect, type ReactNode } from "react";
import { MapContainer, TileLayer, useMap, useMapEvents } from "react-leaflet";
import { latLngBounds } from "leaflet";
import "leaflet/dist/leaflet.css";
import { cx } from "./cx";

export type LatLngTuple = [number, number];

export interface MapViewProps {
  center: LatLngTuple;
  zoom?: number;
  /** When given, the map fits these points into view (and refits when they change). */
  fitTo?: LatLngTuple[];
  /** Pixels at the bottom covered by an overlay (e.g. a BottomSheet), kept clear when fitting. */
  bottomInset?: number;
  onClick?: (position: LatLngTuple) => void;
  children?: ReactNode;
  className?: string;
}

function ClickHandler({ onClick }: { onClick: (p: LatLngTuple) => void }) {
  useMapEvents({ click: (e) => onClick([e.latlng.lat, e.latlng.lng]) });
  return null;
}

function FitBounds({ points, bottomInset }: { points: LatLngTuple[]; bottomInset: number }) {
  const map = useMap();
  const key = points.map((p) => p.join(",")).join("|");
  useEffect(() => {
    if (points.length === 0) return;
    map.fitBounds(latLngBounds(points), {
      paddingTopLeft: [56, 56],
      paddingBottomRight: [56, 56 + bottomInset],
      maxZoom: 16,
    });
    // key captures the identity of points; the array itself changes every render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, bottomInset, map]);
  return null;
}

export function MapView({ center, zoom = 14, fitTo, bottomInset = 0, onClick, children, className }: MapViewProps) {
  return (
    <div className={cx("gm-map", className)}>
      <MapContainer center={center} zoom={zoom} zoomControl={false} style={{ height: "100%", width: "100%" }}>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {onClick && <ClickHandler onClick={onClick} />}
        {fitTo && <FitBounds points={fitTo} bottomInset={bottomInset} />}
        {children}
      </MapContainer>
    </div>
  );
}
