package com.vitalstream.api.reading;

import java.time.Instant;
import java.util.UUID;

/**
 * The message written to the vitals.readings topic, as JSON.
 * This is a contract with every consumer: renaming or removing a field can break them,
 * which is the problem Avro and Schema Registry solve in Phase 3.
 *
 * @param eventId    unique per event, so consumers can detect duplicates
 * @param patientId  looked up at ingest time so consumers don't need to query the devices table
 * @param measuredAt when the device took the reading
 * @param receivedAt when the API accepted it
 */
public record VitalReadingEvent(
        UUID eventId,
        Long deviceId,
        Long patientId,
        Metric metric,
        Double value,
        Instant measuredAt,
        Instant receivedAt) {
}
