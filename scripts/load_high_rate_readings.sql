-- Loads a realistic high-rate dataset for comparing raw-table queries with the read models:
--
--   10 patients (ids 200001-200010), each with
--     - a heart-rate monitor sending 1 reading per second  (devices 200001, 200003, ..., 200019)
--     - a glucose meter taking 1 reading every 4 hours      (devices 200002, 200004, ..., 200020)
--   for the last 24 hours: 864,000 heart-rate + 60 glucose readings, stored in time order.
--
-- These rows are written straight into the processor's table, NOT sent through Kafka, so vitals-query
-- never sees them as events. The second half of this script therefore bootstraps the read models from
-- them directly with SQL, which is how you'd seed a new read model from existing data. (A replay from
-- Kafka won't include them.) They're marked kafka_partition = -2; to remove everything:
--
--   DELETE FROM vitals.vital_readings WHERE kafka_partition = -2;
--   DELETE FROM readmodel.device_minute_stats WHERE device_id BETWEEN 200001 AND 200020;
--   DELETE FROM readmodel.patient_latest_vitals WHERE patient_id BETWEEN 200001 AND 200010;
--
-- Run:  docker exec -i vitalstream-postgres psql -U vitalstream < scripts/load_high_rate_readings.sql

\timing on

-- Raw readings, second by second (all heart-rate devices for second 1, then second 2, ...).
INSERT INTO vitals.vital_readings
    (event_id, device_id, patient_id, metric, value, measured_at, received_at,
     kafka_partition, kafka_offset, firmware_version)
SELECT gen_random_uuid(), 199999 + 2 * p, 200000 + p, 'HEART_RATE',
       round((72 + 12 * sin(extract(epoch FROM t) / 900 + p) + (random() - 0.5) * 6)::numeric, 0),
       t, t, -2, -1, 'synthetic-high-rate'
FROM generate_series(date_trunc('second', now()) - interval '24 hours' + interval '1 second',
                     date_trunc('second', now()), interval '1 second') AS t
CROSS JOIN generate_series(1, 10) AS p;

INSERT INTO vitals.vital_readings
    (event_id, device_id, patient_id, metric, value, measured_at, received_at,
     kafka_partition, kafka_offset, firmware_version)
SELECT gen_random_uuid(), 200000 + 2 * p, 200000 + p, 'GLUCOSE',
       round((100 + (random() - 0.5) * 50)::numeric, 0),
       t, t, -2, -1, 'synthetic-high-rate'
FROM generate_series(date_trunc('hour', now()) - interval '20 hours', date_trunc('hour', now()), interval '4 hours') AS t
CROSS JOIN generate_series(1, 10) AS p;

VACUUM ANALYZE vitals.vital_readings;

-- Bootstrap the read models from those rows: the same results the projector would have produced
-- had each reading arrived as an event.
INSERT INTO readmodel.device_minute_stats (device_id, metric, minute, patient_id, count, sum, min, max)
SELECT device_id, metric, date_trunc('minute', measured_at), patient_id, count(*), sum(value), min(value), max(value)
FROM vitals.vital_readings
WHERE kafka_partition = -2
GROUP BY device_id, metric, date_trunc('minute', measured_at), patient_id;

INSERT INTO readmodel.patient_latest_vitals (patient_id, metric, device_id, value, measured_at, event_id)
SELECT DISTINCT ON (patient_id, metric) patient_id, metric, device_id, value, measured_at, event_id
FROM vitals.vital_readings
WHERE kafka_partition = -2
ORDER BY patient_id, metric, measured_at DESC, event_id DESC;

VACUUM ANALYZE readmodel.device_minute_stats;
VACUUM ANALYZE readmodel.patient_latest_vitals;

SELECT (SELECT count(*) FROM vitals.vital_readings WHERE kafka_partition = -2) AS raw_rows,
       (SELECT count(*) FROM readmodel.device_minute_stats WHERE device_id BETWEEN 200001 AND 200020) AS minute_rows,
       (SELECT count(*) FROM readmodel.patient_latest_vitals WHERE patient_id BETWEEN 200001 AND 200010) AS latest_rows;
