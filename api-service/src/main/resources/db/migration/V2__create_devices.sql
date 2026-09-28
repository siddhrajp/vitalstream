CREATE TABLE devices (
    id            BIGSERIAL    PRIMARY KEY,
    serial_number VARCHAR(64)  NOT NULL UNIQUE,
    device_type   VARCHAR(32)  NOT NULL,          -- e.g. HEART_RATE_MONITOR
    status        VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
    patient_id    BIGINT       NOT NULL REFERENCES patients (id) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Postgres does not index foreign keys automatically; we look devices up by patient often.
CREATE INDEX idx_devices_patient_id ON devices (patient_id);
