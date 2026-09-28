package com.vitalstream.query.kafka;

import com.vitalstream.events.Metric;
import com.vitalstream.events.VitalReadingEvent;
import com.vitalstream.query.readmodel.ReadModelProjector;
import com.vitalstream.query.readmodel.ReadModelProjector.ReadingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Feeds reading events into the read models, a batch at a time (spring.kafka.listener.type: batch).
 * Same topic as vitals-processor, different consumer group, so both services see every event.
 */
@Component
public class ReadingEventListener {

    private final ReadModelProjector projector;

    public ReadingEventListener(ReadModelProjector projector) {
        this.projector = projector;
    }

    /**
     * If record i of the batch is invalid, records 0..i-1 are applied first, then BatchListenerFailedException
     * tells Spring which record failed. Spring commits the offsets before it, hands that one record to the
     * error handler (which dead-letters it), and delivers the records after it again in the next batch.
     * Without the index, Spring would have to treat the whole batch as failed.
     */
    @KafkaListener(topics = "${vitalstream.kafka.topics.readings}")
    public void onReadings(List<ConsumerRecord<String, VitalReadingEvent>> records) {
        List<ReadingEvent> valid = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            try {
                valid.add(validate(records.get(i)));
            } catch (InvalidEventException e) {
                projector.apply(valid);
                throw new BatchListenerFailedException(e.getMessage(), e, i);
            }
        }
        projector.apply(valid);
    }

    private static ReadingEvent validate(ConsumerRecord<String, VitalReadingEvent> record) {
        VitalReadingEvent event = record.value();
        if (event == null) {
            // ErrorHandlingDeserializer couldn't turn the bytes into an event (not valid Avro). The original
            // bytes are kept in a record header, which the dead-letter publisher uses as the DLT message.
            throw new InvalidEventException("Could not deserialize the record at " + where(record));
        }
        UUID eventId;
        try {
            eventId = UUID.fromString(event.getEventId());
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("eventId is not a UUID at " + where(record) + ": " + event.getEventId(), e);
        }
        if (event.getMetric() == Metric.UNKNOWN) {
            throw new InvalidEventException("Unknown metric at " + where(record) + "; this service needs upgrading");
        }
        return new ReadingEvent(event, eventId);
    }

    private static String where(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
