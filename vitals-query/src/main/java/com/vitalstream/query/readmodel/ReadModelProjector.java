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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies reading events to every read model. A "projection" in CQRS terms: it turns the stream of
 * events into tables shaped for reading.
 *
 * Events are applied a batch at a time (up to spring.kafka.consumer.max-poll-records), in one transaction:
 * either the whole batch is applied or none of it, and if the service crashes halfway Kafka redelivers the
 * batch and it's applied cleanly then. Batching is what makes rebuilding from the log fast: instead of
 * three statements per event, a batch of 1,000 events costs one statement to deduplicate plus one upsert
 * per distinct minute bucket and per distinct (patient, metric).
 */
@Service
public class ReadModelProjector {

    private static final Logger log = LoggerFactory.getLogger(ReadModelProjector.class);

    // Records the batch's event ids and returns the ones that weren't there before. Kafka delivers at least
    // once, and "count = count + 1" applied twice gives a wrong answer, so already-applied events are skipped.
    private static final String RECORD_EVENTS = """
            INSERT INTO readmodel.applied_events (event_id)
            SELECT unnest(?::uuid[])
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
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

    // Upsert: start a new minute bucket, or fold this batch's readings for that minute into it. EXCLUDED is
    // the row we tried to insert, i.e. the batch's own count/sum/min/max for the bucket.
    private static final String UPSERT_MINUTE = """
            INSERT INTO readmodel.device_minute_stats (device_id, metric, minute, patient_id, count, sum, min, max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (device_id, metric, minute) DO UPDATE
               SET count = device_minute_stats.count + EXCLUDED.count,
                   sum   = device_minute_stats.sum + EXCLUDED.sum,
                   min   = LEAST(device_minute_stats.min, EXCLUDED.min),
                   max   = GREATEST(device_minute_stats.max, EXCLUDED.max)
            """;

    // Must match how Postgres orders (measured_at, event_id) in UPSERT_LATEST. Postgres compares UUIDs byte
    // by byte, which is the same as comparing their lowercase text form; Java's UUID.compareTo is different
    // (it compares signed numbers), so the text form is used here.
    private static final Comparator<ReadingEvent> NEWEST_LAST =
            Comparator.comparing((ReadingEvent e) -> e.event().getMeasuredAt())
                    .thenComparing(e -> e.eventId().toString());

    private final JdbcTemplate jdbc;
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public ReadModelProjector(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** A reading event together with its already-validated event id. */
    public record ReadingEvent(VitalReadingEvent event, UUID eventId) {
    }

    @Transactional
    public void apply(List<ReadingEvent> batch) {
        if (batch.isEmpty()) {
            return;
        }
        String[] ids = batch.stream().map(e -> e.eventId().toString()).toArray(String[]::new);
        Set<UUID> fresh = new HashSet<>(jdbc.queryForList(RECORD_EVENTS, UUID.class, (Object) ids));

        Map<MinuteKey, MinuteStats> minutes = new LinkedHashMap<>();
        Map<LatestKey, ReadingEvent> latest = new LinkedHashMap<>();
        int applying = 0;
        for (ReadingEvent e : batch) {
            // remove(): if the same event appears twice in one batch, only its first copy is applied.
            if (!fresh.remove(e.eventId())) {
                continue;
            }
            applying++;
            VitalReadingEvent ev = e.event();
            String metric = ev.getMetric().name();
            minutes.computeIfAbsent(
                    new MinuteKey(ev.getDeviceId(), metric, ev.getMeasuredAt().truncatedTo(ChronoUnit.MINUTES)),
                    k -> new MinuteStats(ev.getPatientId())).add(ev.getValue());
            latest.merge(new LatestKey(ev.getPatientId(), metric), e,
                    (a, b) -> NEWEST_LAST.compare(a, b) >= 0 ? a : b);
        }

        List<Object[]> minuteRows = new ArrayList<>(minutes.size());
        minutes.forEach((k, s) -> minuteRows.add(new Object[]{
                k.deviceId(), k.metric(), utc(k.minute()), s.patientId, s.count, s.sum, s.min, s.max}));
        List<Object[]> latestRows = new ArrayList<>(latest.size());
        latest.forEach((k, e) -> latestRows.add(new Object[]{
                k.patientId(), k.metric(), e.event().getDeviceId(), e.event().getValue(),
                utc(e.event().getMeasuredAt()), e.eventId()}));

        // batchUpdate sends all rows of a statement to Postgres in one round trip.
        jdbc.batchUpdate(UPSERT_MINUTE, minuteRows);
        jdbc.batchUpdate(UPSERT_LATEST, latestRows);

        long before = applied.getAndAdd(applying);
        duplicates.addAndGet(batch.size() - applying);
        if ((before + applying) / 10_000 > before / 10_000) {
            log.info("Applied {} events to the read models ({} duplicates skipped); last batch: {} events -> "
                            + "{} minute upserts, {} latest-value upserts",
                    before + applying, duplicates.get(), batch.size(), minuteRows.size(), latestRows.size());
        }
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private record MinuteKey(long deviceId, String metric, Instant minute) {
    }

    private record LatestKey(long patientId, String metric) {
    }

    private static final class MinuteStats {
        final long patientId;
        int count;
        double sum;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        MinuteStats(long patientId) {
            this.patientId = patientId;
        }

        void add(double value) {
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
    }
}
