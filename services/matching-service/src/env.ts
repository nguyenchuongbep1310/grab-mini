export const env = {
  redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  matchRadiusKm: Number(process.env.MATCH_RADIUS_KM ?? 5),
  matchRetries: Number(process.env.MATCH_RETRIES ?? 3),
  matchRetryDelayMs: Number(process.env.MATCH_RETRY_DELAY_MS ?? 2000),
  instanceId: process.env.INSTANCE_ID ?? `matching-${process.pid}`,
};
