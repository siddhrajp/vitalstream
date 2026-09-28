-- Nullable: existing devices have no known firmware version.
ALTER TABLE devices ADD COLUMN firmware_version VARCHAR(32);
