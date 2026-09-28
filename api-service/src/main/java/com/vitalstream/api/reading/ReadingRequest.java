package com.vitalstream.api.reading;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;

import java.time.Instant;

/** Body of POST /api/devices/{deviceId}/readings. measuredAt defaults to the time the API receives it. */
public record ReadingRequest(
        @NotNull Metric metric,
        @NotNull Double value,
        @PastOrPresent Instant measuredAt) {
}
