-- Job 2: driver sessions, a session window over driver.location.updated.
-- A session is one driver's run of location reports; it closes after 30 s
-- without a report from that driver. One row per closed session:
--   flink.driver_sessions {driver_id, session_start, session_end, points}
-- session_end is the time of the driver's last report in the session (in
-- Flink's window TVF, window_end is last report + gap, so the sink subtracts
-- the gap).
--
-- Windows are driven by event time, so a session is written only when the
-- watermark passes its end plus the 30 s gap: that is 30 s of silence from the
-- driver, plus the 5 s watermark delay, plus a later record that moves the
-- watermark (the idle timeout from 01-sources.sql, 5 s, only stops a quiet
-- partition from holding it back; when every partition is idle nothing moves
-- the watermark). So after the last driver stops, the open sessions close only
-- once some driver reports again; with other drivers still reporting that is
-- 30 s + 5 s after the silence.
--
-- The pipeline name is how deploy/flink/submit.sh knows the job already runs.
SET 'pipeline.name' = 'flink-driver-sessions';

-- Job 2 reads the topic with its own consumer group, so Kafka UI shows the
-- lag of each job separately (job 1 uses flink-grab-mini). Everything else is
-- the definition from 01-sources.sql.
CREATE TABLE driver_locations_sessions
WITH ('properties.group.id' = 'flink-grab-mini-sessions')
LIKE driver_locations (OVERWRITING OPTIONS);

-- Demo output in JSON (readable in Kafka UI), append-only, not an event contract.
CREATE TABLE driver_sessions (
  driver_id     STRING,
  session_start TIMESTAMP_LTZ(3),
  session_end   TIMESTAMP_LTZ(3),
  points        BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink.driver_sessions',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

INSERT INTO driver_sessions
SELECT
  driverId                                          AS driver_id,
  CAST(window_start AS TIMESTAMP_LTZ(3))            AS session_start,
  CAST(window_end AS TIMESTAMP_LTZ(3)) - INTERVAL '30' SECOND AS session_end,
  COUNT(*)                                          AS points
FROM TABLE(SESSION(TABLE driver_locations_sessions PARTITION BY driverId, DESCRIPTOR(event_time), INTERVAL '30' SECOND))
GROUP BY driverId, window_start, window_end;
