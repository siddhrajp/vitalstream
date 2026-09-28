-- Read models: tables shaped like the questions they answer, kept up to date from the reading events.
-- They're derived data: everything here can be deleted and rebuilt by replaying the Kafka topic.

-- "What are this patient's latest vitals?"  One row per patient and metric; each new reading overwrites
-- the row if it's newer than what's there. The answer is a primary-key lookup, whatever the history size.
CREATE TABLE patient_latest_vitals (
    patient_id  BIGINT           NOT NULL,
    metric      VARCHAR(32)      NOT NULL,
    device_id   BIGINT           NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    measured_at TIMESTAMPTZ      NOT NULL,
    event_id    UUID             NOT NULL,
    PRIMARY KEY (patient_id, metric)
);

-- "How did this metric behave over time?"  Pre-aggregated per device, metric and minute. Hourly or
-- daily figures are computed by adding up minutes (sum and count add up; min/max take the min/max),
-- which is 60x fewer rows to read than raw readings at one per second, and never needs recomputing.
CREATE TABLE device_minute_stats (
    device_id  BIGINT           NOT NULL,
    metric     VARCHAR(32)      NOT NULL,
    minute     TIMESTAMPTZ      NOT NULL,
    patient_id BIGINT           NOT NULL,
    count      INT              NOT NULL,
    sum        DOUBLE PRECISION NOT NULL,
    min        DOUBLE PRECISION NOT NULL,
    max        DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (device_id, metric, minute)
);

-- Events already applied to the read models. Kafka delivers at least once, and "count = count + 1"
-- applied twice gives a wrong answer, so every update first records its event id here in the same
-- transaction; an id that's already present means the event was applied before and is skipped.
-- It grows with every event; a real system would delete entries older than the longest possible redelivery.
CREATE TABLE applied_events (
    event_id   UUID        PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
