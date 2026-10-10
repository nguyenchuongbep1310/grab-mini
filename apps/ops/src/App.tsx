import { useEffect, useState } from "react";
import { Card, StarRating, Stat, SurgeBadge } from "@grab-mini/ui";
import { loadSnapshot, type Snapshot } from "./stats";

const REFRESH_MS = 3000;

function useSnapshot() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);

  useEffect(() => {
    let stopped = false;
    const refresh = async () => {
      try {
        const next = await loadSnapshot();
        if (stopped) return;
        setSnapshot(next);
        setError(null);
        setUpdatedAt(new Date());
      } catch (err) {
        if (!stopped) setError(err instanceof Error ? err.message : "Couldn't reach ksqlDB");
      }
    };
    refresh();
    const timer = setInterval(refresh, REFRESH_MS);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, []);

  return { snapshot, error, updatedAt };
}

const clock = (ms: number) => new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

export function App() {
  const { snapshot, error, updatedAt } = useSnapshot();

  return (
    <div className="ops gm-root">
      <header className="ops__header">
        <div>
          <h1>grab-mini ops</h1>
          <p>Live figures from ksqlDB, computed from the Kafka topics.</p>
        </div>
        <span className="ops__status" data-state={error ? "error" : snapshot ? "ok" : "loading"}>
          {error ? "ksqlDB unreachable" : snapshot ? `Updated ${updatedAt?.toLocaleTimeString()}` : "Loading…"}
        </span>
      </header>

      {error && <Card className="ops__error">{error}</Card>}

      {snapshot && <Dashboard s={snapshot} />}
    </div>
  );
}

function Dashboard({ s }: { s: Snapshot }) {
  const orders = s.matched + s.noDriver;
  const matchRate = orders === 0 ? null : (s.matched / orders) * 100;
  const finished = s.trips.completed + s.trips.cancelled;
  const maxPerMinute = Math.max(1, ...s.ordersPerMinute.map((m) => m.car + m.bike));

  return (
    <>
      <Card className="ops__stats">
        <Stat label="Orders resolved" value={orders} hint={`${s.matched} matched · ${s.noDriver} without a driver`} />
        <Stat
          label="Match rate"
          value={matchRate === null ? "n/a" : `${matchRate.toFixed(0)}%`}
          tone={matchRate === null ? "default" : matchRate >= 90 ? "success" : matchRate >= 60 ? "warning" : "danger"}
        />
        <Stat
          label="Avg match time"
          value={s.avgMatchMs === null ? "n/a" : `${(s.avgMatchMs / 1000).toFixed(1)}s`}
          hint="order created to driver found"
        />
        <Stat
          label="Trips completed"
          value={s.trips.completed}
          hint={finished === 0 ? "none finished yet" : `${s.trips.cancelled} cancelled · ${s.trips.inProgress} started`}
        />
        <Stat
          label="Location updates"
          value={s.locationUpdatesPerMinute ?? "n/a"}
          hint="per minute, last full minute"
        />
      </Card>

      <div className="ops__grid">
        <Card>
          <h2>Orders per minute</h2>
          {s.ordersPerMinute.length === 0 ? (
            <p className="ops__empty">No orders yet. Book a ride in the Rider App.</p>
          ) : (
            <ul className="bars">
              {s.ordersPerMinute.map((m) => (
                <li key={m.minute}>
                  <span className="bars__label">{clock(m.minute)}</span>
                  <span className="bars__track">
                    <span className="bars__bar bars__bar--car" style={{ width: `${(m.car / maxPerMinute) * 100}%` }} />
                    <span className="bars__bar bars__bar--bike" style={{ width: `${(m.bike / maxPerMinute) * 100}%` }} />
                  </span>
                  <span className="bars__value">{m.car + m.bike}</span>
                </li>
              ))}
            </ul>
          )}
          <div className="legend">
            <span className="legend__dot legend__dot--car" /> Car <span className="legend__dot legend__dot--bike" /> Bike
          </div>
        </Card>

        <Card>
          <h2>Matching by vehicle</h2>
          {s.matchByVehicle.length === 0 ? (
            <p className="ops__empty">No matches yet.</p>
          ) : (
            <table className="table">
              <thead>
                <tr><th>Vehicle</th><th>Matches</th><th>Avg</th><th>Max</th></tr>
              </thead>
              <tbody>
                {s.matchByVehicle.map((v) => (
                  <tr key={v.vehicle}>
                    <td>{v.vehicle === "BIKE" ? "Bike" : "Car"}</td>
                    <td>{v.matches}</td>
                    <td>{(v.avgMs / 1000).toFixed(2)}s</td>
                    <td>{(v.maxMs / 1000).toFixed(2)}s</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>

        <Card>
          <h2>Surge pricing</h2>
          {s.surge.length === 0 ? (
            <p className="ops__empty">No surge data yet.</p>
          ) : (
            <table className="table">
              <thead>
                <tr><th>Minute</th><th>Vehicle</th><th>Orders</th><th>Avg</th><th>Peak</th></tr>
              </thead>
              <tbody>
                {s.surge.map((r) => (
                  <tr key={`${r.minute}-${r.vehicle}`}>
                    <td>{clock(r.minute)}</td>
                    <td>{r.vehicle === "BIKE" ? "Bike" : "Car"}</td>
                    <td>{r.orders}</td>
                    <td>x{r.avgMultiplier.toFixed(1)}</td>
                    <td>{r.maxMultiplier > 1 ? <SurgeBadge multiplier={r.maxMultiplier} label="" /> : "x1.0"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>

        <Card>
          <h2>Driver ratings</h2>
          {s.ratings.length === 0 ? (
            <p className="ops__empty">No ratings yet.</p>
          ) : (
            <table className="table">
              <thead>
                <tr><th>Driver</th><th>Rating</th><th>Count</th></tr>
              </thead>
              <tbody>
                {s.ratings.map((r) => (
                  <tr key={r.driverId}>
                    <td>{r.driverId}</td>
                    <td className="table__stars">
                      <StarRating value={r.average} size="sm" label={`${r.driverId} rating`} /> {r.average.toFixed(2)}
                    </td>
                    <td>{r.count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      </div>
    </>
  );
}
