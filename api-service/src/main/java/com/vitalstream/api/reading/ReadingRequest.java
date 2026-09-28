package com.vitalstream.api.reading;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;

import java.time.Instant;
import java.util.UUID;

/**
 * Body of POST /api/devices/{deviceId}/readings (and what the gRPC request is converted into).
 * measuredAt defaults to the time the API receives it.
 * readingId is an optional client-chosen idempotency key: it becomes the event id, so sending the same
 * reading twice (e.g. retrying after a timeout) stores it once. If omitted, a random id is generated.
 */
public record ReadingRequest(
        @NotNull Metric metric,
        @NotNull Double value,
        @PastOrPresent Instant measuredAt,
        UUID readingId) {
}
