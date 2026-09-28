-- Matches the optional firmwareVersion field added in VitalReadingEvent schema v2.
-- Nullable: readings stored before this, and events written with schema v1, have no firmware version.
ALTER TABLE vital_readings ADD COLUMN firmware_version VARCHAR(32);
