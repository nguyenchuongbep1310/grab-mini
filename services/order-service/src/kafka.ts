import { Kafka } from "kafkajs";
import { env } from "./env.js";

export const kafka = new Kafka({
  clientId: "order-service",
  brokers: env.kafkaBrokers,
});

export const producer = kafka.producer();
