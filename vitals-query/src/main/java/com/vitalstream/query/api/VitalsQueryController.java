package com.vitalstream.query.api;

import com.vitalstream.query.api.VitalsQueries.Bucket;
import com.vitalstream.query.api.VitalsQueries.LatestVital;
import com.vitalstream.query.api.VitalsQueries.StatsBucket;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * The query side of CQRS: read-only endpoints answered entirely from the read models.
 *
 * Results are eventually consistent: a reading accepted by api-service shows up here once this service
 * has consumed its event (normally ~10 ms later, longer if this service is catching up). The read side
 * also doesn't know which patients or devices exist (that's api-service's data), so an unknown id simply
 * has no data: an empty list, not a 404.
 */
@RestController
public class VitalsQueryController {

    private static final Duration DEFAULT_RANGE = Duration.ofHours(24);
    private static final Duration MAX_RANGE = Duration.ofDays(31);

    private final VitalsQueries queries;

    public VitalsQueryController(VitalsQueries queries) {
        this.queries = queries;
    }

    /** Latest value of every metric the patient has, e.g. GET /api/patients/3/vitals/latest */
    @GetMapping("/api/patients/{patientId}/vitals/latest")
    public List<LatestVital> latest(@PathVariable long patientId) {
        return queries.latestForPatient(patientId);
    }

    /**
     * Count/avg/min/max per minute, hour or day, e.g.
     * GET /api/devices/200001/vitals/HEART_RATE/stats?bucket=hour  (defaults: last 24 hours, hourly)
     * GET /api/devices/4/vitals/heart_rate/stats?bucket=minute&from=2026-09-28T15:00:00Z&to=2026-09-28T16:00:00Z
     */
    @GetMapping("/api/devices/{deviceId}/vitals/{metric}/stats")
    public StatsResponse stats(@PathVariable long deviceId,
                               @PathVariable String metric,
                               @RequestParam(defaultValue = "hour") String bucket,
                               @RequestParam(required = false) Instant from,
                               @RequestParam(required = false) Instant to) {
        Bucket size = parseBucket(bucket);
        Instant end = to != null ? to : Instant.now();
        Instant start = from != null ? from : end.minus(DEFAULT_RANGE);
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("'from' must be before 'to'");
        }
        // Protects the database from accidentally huge requests (a year of minutes is 525,600 rows).
        if (Duration.between(start, end).compareTo(MAX_RANGE) > 0) {
            throw new IllegalArgumentException("range may be at most " + MAX_RANGE.toDays() + " days");
        }
        String metricName = metric.toUpperCase(Locale.ROOT);
        List<StatsBucket> buckets = queries.stats(deviceId, metricName, size, start, end);
        return new StatsResponse(deviceId, metricName, size.name().toLowerCase(Locale.ROOT), start, end, buckets);
    }

    private static Bucket parseBucket(String bucket) {
        try {
            return Bucket.valueOf(bucket.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("bucket must be one of minute, hour, day");
        }
    }

    public record StatsResponse(long deviceId, String metric, String bucket, Instant from, Instant to,
                                List<StatsBucket> buckets) {
    }
}
