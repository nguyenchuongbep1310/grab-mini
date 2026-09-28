export const env = {
  port: Number(process.env.PORT ?? 3001),
  databaseUrl: process.env.DATABASE_URL ?? "postgres://grabmini:grabmini@localhost:5432/grabmini",
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
};
