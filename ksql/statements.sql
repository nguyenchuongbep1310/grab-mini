-- ksqlDB analytics for grab-mini. Run automatically by the ksql-init service.
--
-- Columns are inferred from the Avro schemas in the Schema Registry (ksqlDB
-- upper-cases them: orderId becomes ORDERID). Everything here is
-- IF NOT EXISTS so it can be applied again safely.

SET 'auto.offset.reset' = 'earliest';

-- ---------------------------------------------------------------------------
-- Sources: one stream per Kafka topic. The message key is the entity id.
-- ---------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS orders_created (order_key VARCHAR KEY)
  WITH (KAFKA_TOPIC = 'order.created', VALUE_FORMAT = 'AVRO');

CREATE STREAM IF NOT EXISTS orders_matched (order_key VARCHAR KEY)
  WITH (KAFKA_TOPIC = 'order.matched', VALUE_FORMAT = 'AVRO');

CREATE STREAM IF NOT EXISTS orders_no_driver (order_key VARCHAR KEY)
  WITH (KAFKA_TOPIC = 'order.no_driver_found', VALUE_FORMAT = 'AVRO');

CREATE STREAM IF NOT EXISTS trip_status (trip_key VARCHAR KEY)
  WITH (KAFKA_TOPIC = 'trip.status.changed', VALUE_FORMAT = 'AVRO');

CREATE STREAM IF NOT EXISTS trip_ratings (driver_key VARCHAR KEY)
  WITH (KAFKA_TOPIC = 'trip.rated', VALUE_FORMAT = 'AVRO');

CREATE STREAM IF NOT EXISTS driver_locations (driver_key VARCHAR KEY)
  WITH (KAFKA_TOPIC = 'driver.location.updated', VALUE_FORMAT = 'AVRO');

-- ---------------------------------------------------------------------------
-- Orders per minute, by vehicle type (a tumbling window per minute).
-- COALESCE covers messages written before vehicleType existed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders_per_minute AS
  SELECT COALESCE(vehicleType, 'CAR') AS vehicle_type,
         COUNT(*) AS orders
  FROM orders_created
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY COALESCE(vehicleType, 'CAR')
  EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Surge pricing: per minute and vehicle type, how many orders and what
-- multiplier they were booked at. COALESCE covers order.created messages
-- written before schema v3 (no surgeMultiplier field: those were 1.0).
-- Note: the orders_created stream's columns are inferred when it is first
-- created, so a ksqlDB that already ran needs `scripts/dev.sh --reset` (or the
-- stream dropped and recreated) to see the surge columns.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS surge_demand AS
  SELECT COALESCE(vehicleType, 'CAR') AS vehicle_type,
         COUNT(*) AS orders,
         AVG(COALESCE(surgeMultiplier, 1.0)) AS avg_multiplier,
         MAX(COALESCE(surgeMultiplier, 1.0)) AS max_multiplier
  FROM orders_created
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY COALESCE(vehicleType, 'CAR')
  EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- How long matching takes: join each order.created to its order.matched
-- (within a minute) and measure the gap between their record timestamps.
-- ---------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS match_latency AS
  SELECT m.orderId AS order_id,
         COALESCE(c.vehicleType, 'CAR') AS vehicle_type,
         m.ROWTIME - c.ROWTIME AS match_ms
  FROM orders_created c
  INNER JOIN orders_matched m WITHIN 1 MINUTE ON c.orderId = m.orderId
  EMIT CHANGES;

CREATE TABLE IF NOT EXISTS match_stats AS
  SELECT vehicle_type,
         COUNT(*) AS matches,
         CAST(AVG(match_ms) AS BIGINT) AS avg_match_ms,
         MAX(match_ms) AS max_match_ms
  FROM match_latency
  GROUP BY vehicle_type
  EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Outcomes: how many orders found a driver, and how many did not.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS matched_total AS
  SELECT 'ALL' AS scope, COUNT(*) AS orders
  FROM orders_matched
  GROUP BY 'ALL'
  EMIT CHANGES;

CREATE TABLE IF NOT EXISTS no_driver_total AS
  SELECT 'ALL' AS scope, COUNT(*) AS orders
  FROM orders_no_driver
  GROUP BY 'ALL'
  EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Trips by status, e.g. completed versus cancelled.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trip_status_totals AS
  SELECT status AS status, COUNT(*) AS changes
  FROM trip_status
  GROUP BY status
  EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Driver ratings, the same figure Rating Service keeps in Postgres.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS driver_rating_stats AS
  SELECT driverId AS driver_id,
         COUNT(*) AS ratings,
         AVG(CAST(stars AS DOUBLE)) AS avg_stars
  FROM trip_ratings
  GROUP BY driverId
  EMIT CHANGES;

-- ---------------------------------------------------------------------------
-- Location updates per minute: the busiest topic, as a throughput figure.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS location_updates_per_minute AS
  SELECT 'ALL' AS scope, COUNT(*) AS updates
  FROM driver_locations
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY 'ALL'
  EMIT CHANGES;
