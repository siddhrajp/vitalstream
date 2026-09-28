package com.vitalstream.api.reading;

import com.vitalstream.events.VitalReadingEvent;

import java.time.Instant;

/**
 * Response body for an accepted reading. Kept separate from the generated Avro class, which is
 * the Kafka contract: Avro classes carry schema metadata that Jackson can't turn into sensible JSON,
 * and the REST API shouldn't change shape whenever the event schema does.
 */
public record ReadingAccepted(
        String eventId,
        Long deviceId,
        Long patientId,
        String metric,
        Double value,
        Instant measuredAt,
        Instant receivedAt) {

    static ReadingAccepted from(VitalReadingEvent e) {
        return new ReadingAccepted(e.getEventId(), e.getDeviceId(), e.getPatientId(), e.getMetric().name(),
                e.getValue(), e.getMeasuredAt(), e.getReceivedAt());
    }
}
