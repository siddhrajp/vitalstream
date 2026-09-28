package com.vitalstream.query.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Read-only SQL against the read models. Each query touches only the rows it returns (or, for stats,
 * the minute rows it adds up), no matter how many raw readings exist.
 */
@Repository
public class VitalsQueries {

    private static final String LATEST = """
            SELECT metric, value, device_id, measured_at
            FROM readmodel.patient_latest_vitals
            WHERE patient_id = ?
            ORDER BY metric
            """;

    // Hourly/daily figures are combined from minute rows: counts and sums add up, min/max of mins/maxes.
    // The average is recomputed as total sum / total count (averaging the minute averages would be
    // wrong whenever minutes hold different numbers of readings).
    // date_trunc(..., 'UTC') makes hour and day boundaries UTC, whatever the database session's time zone.
    private static final String STATS = """
            SELECT date_trunc(?, minute, 'UTC') AS bucket_start,
                   sum(count)            AS count,
                   sum(sum) / sum(count) AS avg,
                   min(min)              AS min,
                   max(max)              AS max
            FROM readmodel.device_minute_stats
            WHERE device_id = ? AND metric = ? AND minute >= ? AND minute < ?
            GROUP BY 1
            ORDER BY 1
            """;

    private final JdbcTemplate jdbc;

    public VitalsQueries(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<LatestVital> latestForPatient(long patientId) {
        return jdbc.query(LATEST, (rs, i) -> new LatestVital(
                rs.getString("metric"),
                rs.getDouble("value"),
                rs.getLong("device_id"),
                rs.getObject("measured_at", OffsetDateTime.class).toInstant()), patientId);
    }

    public List<StatsBucket> stats(long deviceId, String metric, Bucket bucket, Instant from, Instant to) {
        return jdbc.query(STATS, (rs, i) -> new StatsBucket(
                rs.getObject("bucket_start", OffsetDateTime.class).toInstant(),
                rs.getLong("count"),
                Math.round(rs.getDouble("avg") * 10) / 10.0,
                rs.getDouble("min"),
                rs.getDouble("max")),
                bucket.sqlUnit(), deviceId, metric, utc(from), utc(to));
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    public record LatestVital(String metric, double value, long deviceId, Instant measuredAt) {
    }

    public record StatsBucket(Instant start, long count, double avg, double min, double max) {
    }

    public enum Bucket {
        MINUTE, HOUR, DAY;

        String sqlUnit() {
            return name().toLowerCase();
        }
    }
}
