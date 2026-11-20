-- Flink SQL demo: source tables and session settings shared by every job.
--
-- This file starts no job. deploy/flink/submit.sh puts it in front of each
-- job file (02-..., 03-...), because every SQL client run is a new session
-- and these tables live in the session's in-memory catalog.
--
-- Like ksql/statements.sql, the sources read the Avro topics through Schema
-- Registry and take the Kafka record timestamp as event time (ksqlDB's
-- ROWTIME), from the earliest offset, so the two engines see the same data in
-- the same windows. Only the columns the jobs need are declared: Flink derives
-- an Avro reader schema from them and Avro schema resolution skips the other
-- fields of the writer's schema (fetched from the registry by the id in each
-- message), so no schema subject has to exist when the job starts.

-- Event time is the record timestamp and windows are aligned to UTC.
SET 'table.local-time-zone' = 'UTC';

-- order.created and driver.location.updated have 3 partitions. A partition
-- that receives nothing for 5 s stops holding the watermark back. Without
-- this, one quiet partition keeps every window open forever.
SET 'table.exec.source.idle-timeout' = '5 s';

CREATE TABLE orders_created (
  orderId     STRING,
  -- The schema defaults vehicleType to CAR, but the jobs still COALESCE it, as
  -- ksql/statements.sql does.
  vehicleType STRING,
  event_time  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'order.created',
  'properties.bootstrap.servers' = 'kafka:19092',
  -- Offsets are committed on each checkpoint under this group, so the
  -- job's lag shows in Kafka UI and `make kafka-lag`. The job does not
  -- start from them: a restore uses the checkpoint, a new job the earliest.
  'properties.group.id' = 'flink-grab-mini',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE driver_locations (
  driverId   STRING,
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'driver.location.updated',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-grab-mini',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);
