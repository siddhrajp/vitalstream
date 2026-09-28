package com.vitalstream.processor.reading;

import com.vitalstream.events.VitalReadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ReadingStore {

    private static final Logger log = LoggerFactory.getLogger(ReadingStore.class);

    private final VitalReadingRepository readings;

    public ReadingStore(VitalReadingRepository readings) {
        this.readings = readings;
    }

    /**
     * Kafka delivers "at least once": after a crash or rebalance, messages whose offsets weren't
     * committed yet are delivered again. Skipping event ids we've already stored makes a repeat harmless.
     * Only one thread handles a given partition, and a device's events all go to one partition, so the
     * same event can't be processed by two threads at once; the UNIQUE constraint is the backstop.
     */
    @Transactional
    public void store(VitalReadingEvent event, UUID eventId, int partition, long offset) {
        if (readings.existsByEventId(eventId)) {
            log.info("Skipping duplicate event {} ({}@{})", eventId, partition, offset);
            return;
        }
        readings.save(new VitalReading(event, eventId, partition, offset));
        log.info("Stored {} {} for device {} (partition {}, offset {})",
                event.getMetric(), event.getValue(), event.getDeviceId(), partition, offset);
    }
}
