import express from "express";
import { randomUUID } from "node:crypto";
import { Topics, type OrderCreated } from "@grab-mini/events";
import { env } from "./env.js";
import { pool, ensureSchema } from "./db.js";
import { producer } from "./kafka.js";

interface LatLng {
  lat: number;
  lng: number;
}

function isValidLatLng(value: unknown): value is LatLng {
  if (typeof value !== "object" || value === null) return false;
  const { lat, lng } = value as Record<string, unknown>;
  return typeof lat === "number" && typeof lng === "number" && Math.abs(lat) <= 90 && Math.abs(lng) <= 180;
}

async function main() {
  await ensureSchema();
  await producer.connect();

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => res.json({ status: "ok" }));

  // US-1.1: create an order and publish order.created
  app.post("/orders", async (req, res) => {
    const { riderId, pickup, dropoff } = req.body ?? {};

    if (typeof riderId !== "string" || !riderId || !isValidLatLng(pickup) || !isValidLatLng(dropoff)) {
      return res.status(400).json({ error: "riderId, pickup {lat,lng} and dropoff {lat,lng} are required" });
    }

    const orderId = randomUUID();

    await pool.query(
      `INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')`,
      [orderId, riderId, pickup.lat, pickup.lng, dropoff.lat, dropoff.lng],
    );

    const event: OrderCreated = {
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      orderId,
      riderId,
      pickup,
      dropoff,
    };

    await producer.send({
      topic: Topics.OrderCreated,
      messages: [{ key: orderId, value: JSON.stringify(event) }],
    });

    res.status(201).json({ orderId, status: "PENDING" });
  });

  app.listen(env.port, () => {
    console.log(`order-service listening on :${env.port}`);
  });
}

main().catch((err) => {
  console.error("order-service failed to start", err);
  process.exit(1);
});
