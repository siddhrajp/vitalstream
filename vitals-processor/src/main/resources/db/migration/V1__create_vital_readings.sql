-- Owned by vitals-processor, in its own "vitals" schema. There is deliberately no foreign key to
-- public.devices / public.patients: those tables belong to api-service, and this service must keep
-- working (and accepting events) even if that schema changes or lives in another database later.
CREATE TABLE vital_readings (
    id              BIGSERIAL        PRIMARY KEY,
    event_id        UUID             NOT NULL UNIQUE,   -- makes processing idempotent: a redelivered event can't be stored twice
    device_id       BIGINT           NOT NULL,
    patient_id      BIGINT           NOT NULL,
    metric          VARCHAR(32)      NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    measured_at     TIMESTAMPTZ      NOT NULL,
    received_at     TIMESTAMPTZ      NOT NULL,
    processed_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    kafka_partition INT              NOT NULL,          -- where the event came from, for tracing
    kafka_offset    BIGINT           NOT NULL
);

-- The typical questions: "readings for this patient over time" and "readings from this device over time".
CREATE INDEX idx_vital_readings_patient_time ON vital_readings (patient_id, measured_at);
CREATE INDEX idx_vital_readings_device_time  ON vital_readings (device_id, measured_at);
