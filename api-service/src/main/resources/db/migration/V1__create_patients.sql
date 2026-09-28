CREATE TABLE patients (
    id            BIGSERIAL    PRIMARY KEY,
    mrn           VARCHAR(32)  NOT NULL UNIQUE,   -- medical record number
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    date_of_birth DATE         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
