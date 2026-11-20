-- Job 1: orders per minute by vehicle type, the twin of ksqlDB's
-- orders_per_minute table (ksql/statements.sql). Compare a finished minute:
--   ksqlDB: SELECT * FROM orders_per_minute
--   Flink:  the flink.orders_per_minute topic in Kafka UI
--
-- Flink writes each window once, when the watermark passes its end (about
-- 5 s after the minute, once a later record arrives), while ksqlDB updates its
-- table with every order. The sink is upsert-kafka, so a resubmitted job that
-- recounts from the earliest offset overwrites the same keys.
--
-- The pipeline name is how deploy/flink/submit.sh knows the job already runs.
SET 'pipeline.name' = 'flink-orders-per-minute';

-- Demo output in JSON (readable in Kafka UI), not an event contract.
-- Key: {"window_start":...,"vehicle_type":...}, value: all four columns.
CREATE TABLE orders_per_minute (
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  vehicle_type STRING,
  orders       BIGINT,
  PRIMARY KEY (window_start, vehicle_type) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'flink.orders_per_minute',
  'properties.bootstrap.servers' = 'kafka:19092',
  'key.format' = 'json',
  'key.json.timestamp-format.standard' = 'ISO-8601',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

INSERT INTO orders_per_minute
SELECT
  CAST(window_start AS TIMESTAMP_LTZ(3)) AS window_start,
  CAST(window_end AS TIMESTAMP_LTZ(3))   AS window_end,
  COALESCE(vehicleType, 'CAR')           AS vehicle_type,
  COUNT(*)                               AS orders
FROM TABLE(TUMBLE(TABLE orders_created, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, COALESCE(vehicleType, 'CAR');
