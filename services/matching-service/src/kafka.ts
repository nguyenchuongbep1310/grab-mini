import { Kafka } from "kafkajs";
import { env } from "./env.js";

export const kafka = new Kafka({
  clientId: "matching-service",
  brokers: env.kafkaBrokers,
});

export const consumer = kafka.consumer({ groupId: "matching-service-group" });
export const producer = kafka.producer();
