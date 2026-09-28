-- Same questions, answered from the raw table (best query from step 1) and from the read models.
-- Uses the high-rate dataset (scripts/load_high_rate_readings.sql): patient 200001 has a heart-rate
-- monitor (device 200001, 1 reading/second) and a glucose meter (device 200002, every 4 hours).
--
-- Run:  docker exec -i vitalstream-postgres psql -U vitalstream < scripts/compare_read_models.sql

\echo '===== Latest vitals, RAW table, LATERAL (the fast version from step 1)'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT m.metric, r.value, r.measured_at
FROM unnest(ARRAY['HEART_RATE', 'SPO2', 'BP_SYSTOLIC', 'BP_DIASTOLIC', 'TEMPERATURE', 'GLUCOSE']) AS m(metric)
CROSS JOIN LATERAL (
    SELECT value, measured_at FROM vitals.vital_readings
    WHERE patient_id = 200001 AND metric = m.metric
    ORDER BY measured_at DESC LIMIT 1
) AS r;

\echo '===== Latest vitals, READ MODEL'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT metric, value, measured_at FROM readmodel.patient_latest_vitals WHERE patient_id = 200001;

\echo '===== Hourly heart rate for 24 hours, RAW table'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT date_trunc('hour', measured_at) AS hour, count(*), avg(value), min(value), max(value)
FROM vitals.vital_readings
WHERE device_id = 200001 AND metric = 'HEART_RATE' AND measured_at >= now() - interval '24 hours'
GROUP BY 1 ORDER BY 1;

\echo '===== Hourly heart rate for 24 hours, READ MODEL (adds up 60 minute rows per hour)'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT date_trunc('hour', minute) AS hour, sum(count), sum(sum) / sum(count) AS avg, min(min), max(max)
FROM readmodel.device_minute_stats
WHERE device_id = 200001 AND metric = 'HEART_RATE' AND minute >= now() - interval '24 hours'
GROUP BY 1 ORDER BY 1;
