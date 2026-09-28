-- Matches the optional submittedBy field added in VitalReadingEvent schema v3: who sent the reading
-- (the preferred_username from the caller's access token). Null for readings from before authentication.
ALTER TABLE vital_readings ADD COLUMN submitted_by VARCHAR(255);
