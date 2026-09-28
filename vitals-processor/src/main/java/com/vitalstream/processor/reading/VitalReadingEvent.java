package com.vitalstream.processor.reading;

import java.time.Instant;
import java.util.UUID;

/**
 * This service's copy of the event api-service publishes. The two services share no code, only the
 * JSON format on the topic, so this class must be kept in step with api-service by hand for now.
 * Phase 3 replaces that with an Avro schema both sides are generated from.
 *
 * metric is a String rather than an enum on purpose: if api-service adds a new metric, this service
 * keeps storing readings instead of failing on a value it doesn't know ("tolerant reader").
 */
public record VitalReadingEvent(
        UUID eventId,
        Long deviceId,
        Long patientId,
        String metric,
        Double value,
        Instant measuredAt,
        Instant receivedAt) {

    /** Jackson fills missing fields with null, so a message can parse as JSON and still be unusable. */
    boolean isComplete() {
        return eventId != null && deviceId != null && patientId != null && metric != null
                && value != null && measuredAt != null && receivedAt != null;
    }
}
