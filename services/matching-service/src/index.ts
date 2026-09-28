import { randomUUID } from "node:crypto";
import { Topics, type OrderCreated, type OrderMatched, type OrderNoDriverFound } from "@grab-mini/events";
import { env } from "./env.js";
import { consumer, producer } from "./kafka.js";
import { findNearestAvailableDriver, setDriverStatus } from "./redis.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// US-2.1: try to match, retrying for a bounded time before giving up.
async function matchOrder(order: OrderCreated): Promise<void> {
  let driverId: string | null = null;

  for (let attempt = 0; attempt <= env.matchRetries && !driverId; attempt++) {
    if (attempt > 0) await sleep(env.matchRetryDelayMs);
    driverId = await findNearestAvailableDriver(order.pickup.lat, order.pickup.lng, env.matchRadiusKm);
  }

  if (driverId) {
    await setDriverStatus(driverId, "ASSIGNED");
    const event: OrderMatched = {
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      orderId: order.orderId,
      driverId,
    };
    await producer.send({ topic: Topics.OrderMatched, messages: [{ key: order.orderId, value: JSON.stringify(event) }] });
    console.log(`[${env.instanceId}] matched order=${order.orderId} -> driver=${driverId}`);
  } else {
    const event: OrderNoDriverFound = {
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      orderId: order.orderId,
    };
    await producer.send({
      topic: Topics.OrderNoDriverFound,
      messages: [{ key: order.orderId, value: JSON.stringify(event) }],
    });
    console.log(`[${env.instanceId}] no driver found for order=${order.orderId}`);
  }
}

async function main() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: Topics.OrderCreated, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      if (!message.value) return;
      const order = JSON.parse(message.value.toString()) as OrderCreated;
      console.log(
        `[${env.instanceId}] received order.created partition=${partition} offset=${message.offset} orderId=${order.orderId}`,
      );
      await matchOrder(order);
    },
  });

  console.log(`[${env.instanceId}] matching-service consumer running`);
}

main().catch((err) => {
  console.error("matching-service failed to start", err);
  process.exit(1);
});
