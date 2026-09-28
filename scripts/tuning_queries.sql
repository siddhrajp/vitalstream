-- The three dashboard queries used for tuning, with EXPLAIN (ANALYZE, BUFFERS): Postgres runs each query
-- and reports the plan it chose, the time spent in each step, and how many 8 KB pages it touched.
-- Patient 100100 owns devices 100601-100606; device 100602 measures HEART_RATE (synthetic range 57-87 bpm).
--
-- Run:  docker exec -i vitalstream-postgres psql -U vitalstream < scripts/tuning_queries.sql

SET search_path = vitals;

\echo '===== Q1a: latest value of each metric for one patient (DISTINCT ON)'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT DISTINCT ON (metric) metric, value, measured_at
FROM vital_readings
WHERE patient_id = 100100
ORDER BY metric, measured_at DESC;

\echo '===== Q1b: same answer, one small lookup per metric (LATERAL ... LIMIT 1)'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT m.metric, r.value, r.measured_at
FROM unnest(ARRAY['HEART_RATE', 'SPO2', 'BP_SYSTOLIC', 'BP_DIASTOLIC', 'TEMPERATURE', 'GLUCOSE']) AS m(metric)
CROSS JOIN LATERAL (
    SELECT value, measured_at
    FROM vital_readings
    WHERE patient_id = 100100 AND metric = m.metric
    ORDER BY measured_at DESC
    LIMIT 1
) AS r;

\echo '===== Q2: hourly min/avg/max heart rate for one device, last 24 hours'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT date_trunc('hour', measured_at) AS hour, min(value), round(avg(value)::numeric, 1) AS avg, max(value), count(*)
FROM vital_readings
WHERE device_id = 100602 AND metric = 'HEART_RATE' AND measured_at >= now() - interval '24 hours'
GROUP BY 1
ORDER BY 1;

\echo '===== Q3: every heart rate above 85 in the last 5 minutes, across all devices'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT device_id, patient_id, value, measured_at
FROM vital_readings
WHERE measured_at >= now() - interval '5 minutes' AND metric = 'HEART_RATE' AND value > 85
ORDER BY measured_at DESC;
