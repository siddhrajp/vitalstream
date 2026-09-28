-- Loads ~5 million synthetic readings into vitals.vital_readings for query-tuning experiments.
--
--   2,000 devices (ids 100001-102000) on 333 patients (ids 100000-100333), 6 devices per patient, one
--   metric per device, one reading per device per minute for the last 2,500 minutes (~41 hours).
--
-- Rows are generated minute by minute (all devices for minute 1, then minute 2, ...) so they're stored
-- in time order, the way real readings arrive. Synthetic rows are marked kafka_partition = -1 and use
-- ids far above the real ones, so they're easy to tell apart and remove:
--
--   DELETE FROM vitals.vital_readings WHERE kafka_partition = -1;
--
-- Run:  docker exec -i vitalstream-postgres psql -U vitalstream < scripts/load_synthetic_readings.sql

\timing on

INSERT INTO vitals.vital_readings
    (event_id, device_id, patient_id, metric, value, measured_at, received_at,
     kafka_partition, kafka_offset, firmware_version)
SELECT gen_random_uuid(),
       d,
       100000 + (d - 100001) / 6,
       m.metric,
       round((m.base + (random() - 0.5) * 2 * m.spread)::numeric, 1),
       t,
       t,
       -1,
       -1,
       'synthetic'
FROM generate_series(date_trunc('minute', now()) - interval '2499 minutes',
                     date_trunc('minute', now()), interval '1 minute') AS t
CROSS JOIN generate_series(100001, 102000) AS d
CROSS JOIN LATERAL (
    SELECT (ARRAY['HEART_RATE', 'SPO2', 'BP_SYSTOLIC', 'BP_DIASTOLIC', 'TEMPERATURE', 'GLUCOSE'])[1 + d % 6] AS metric,
           (ARRAY[72, 97, 120, 78, 36.8, 100])[1 + d % 6] AS base,
           (ARRAY[15, 2, 15, 10, 0.5, 25])[1 + d % 6] AS spread
) AS m;

-- Refresh the planner's statistics (row counts, value distributions) so its choices reflect the new data,
-- and the visibility map, which index-only scans rely on.
VACUUM ANALYZE vitals.vital_readings;

SELECT count(*) AS total_rows,
       count(*) FILTER (WHERE kafka_partition = -1) AS synthetic_rows,
       pg_size_pretty(pg_table_size('vitals.vital_readings')) AS table_size,
       pg_size_pretty(pg_indexes_size('vitals.vital_readings')) AS index_size
FROM vitals.vital_readings;
