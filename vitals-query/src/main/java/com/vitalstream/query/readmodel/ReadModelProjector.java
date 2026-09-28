package com.vitalstream.query.readmodel;

import com.vitalstream.events.VitalReadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies one reading event to every read model. A "projection" in CQRS terms: it turns the stream of
 * events into tables shaped for reading.
 *
 * All three statements run in one transaction, so an event is either fully applied or not at all;
 * if the service crashes halfway, Kafka redelivers the event and it's applied cleanly then.
 */
@Service
public class ReadModelProjector {

    private static final Logger log = LoggerFactory.getLogger(ReadModelProjector.class);

    private static final String RECORD_EVENT = """
            INSERT INTO readmodel.applied_events (event_id) VALUES (?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    // Upsert: insert the row, or if the patient already has one for this metric, overwrite it, but only
    // when this reading is newer. Events can arrive out of order (retries, replays, several partitions),
    // and an older reading must never replace a newer one.
    // Two readings can share a millisecond; comparing (measured_at, event_id) as a pair breaks such ties
    // the same way whatever order they arrive in, so a rebuild always produces the same table.
    private static final String UPSERT_LATEST = """
            INSERT INTO readmodel.patient_latest_vitals (patient_id, metric, device_id, value, measured_at, event_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (patient_id, metric) DO UPDATE
               SET device_id = EXCLUDED.device_id, value = EXCLUDED.value,
                   measured_at = EXCLUDED.measured_at, event_id = EXCLUDED.event_id
             WHERE (patient_latest_vitals.measured_at, patient_latest_vitals.event_id)
                 < (EXCLUDED.measured_at, EXCLUDED.event_id)
            """;

    // Upsert: start a new minute bucket, or fold this reading into the existing one. EXCLUDED is the row
    // we tried to insert, i.e. this single reading.
    private static final String UPSERT_MINUTE = """
            INSERT INTO readmodel.device_minute_stats (device_id, metric, minute, patient_id, count, sum, min, max)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT (device_id, metric, minute) DO UPDATE
               SET count = device_minute_stats.count + 1,
                   sum   = device_minute_stats.sum + EXCLUDED.sum,
                   min   = LEAST(device_minute_stats.min, EXCLUDED.min),
                   max   = GREATEST(device_minute_stats.max, EXCLUDED.max)
            """;

    private final JdbcTemplate jdbc;
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public ReadModelProjector(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void apply(VitalReadingEvent event, UUID eventId) {
        if (jdbc.update(RECORD_EVENT, eventId) == 0) {
            duplicates.incrementAndGet();
            log.debug("Skipping already-applied event {}", eventId);
            return;
        }

        String metric = event.getMetric().name();
        OffsetDateTime measuredAt = utc(event.getMeasuredAt());
        OffsetDateTime minute = utc(event.getMeasuredAt().truncatedTo(ChronoUnit.MINUTES));
        double value = event.getValue();

        jdbc.update(UPSERT_LATEST, event.getPatientId(), metric, event.getDeviceId(), value, measuredAt, eventId);
        jdbc.update(UPSERT_MINUTE, event.getDeviceId(), metric, minute, event.getPatientId(), value, value, value);

        long n = applied.incrementAndGet();
        if (n % 10_000 == 0) {
            log.info("Applied {} events to the read models ({} duplicates skipped)", n, duplicates.get());
        }
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
