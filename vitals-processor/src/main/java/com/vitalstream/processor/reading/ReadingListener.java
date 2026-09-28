package com.vitalstream.processor.reading;

import com.vitalstream.events.Metric;
import com.vitalstream.events.VitalReadingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ReadingListener {

    private final ReadingStore store;

    public ReadingListener(ReadingStore store) {
        this.store = store;
    }

    /**
     * Spring runs a loop per listener thread: poll Kafka for a batch of records, call this method once per
     * record, then commit the offsets of the batch. If this method throws, the offset is not committed and
     * the error handler in KafkaErrorHandlingConfig decides whether to retry or dead-letter the record.
     *
     * By the time a record gets here the Avro deserializer has already checked it against its schema, so
     * every field is present and correctly typed. Messages that aren't valid Avro never reach this method;
     * they go straight to the dead-letter topic. What's left to check is meaning, not shape.
     */
    @KafkaListener(topics = "${vitalstream.kafka.topics.readings}")
    public void onReading(ConsumerRecord<String, VitalReadingEvent> record) {
        VitalReadingEvent event = record.value();

        UUID eventId;
        try {
            eventId = UUID.fromString(event.getEventId());
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("eventId is not a UUID at " + where(record) + ": " + event.getEventId(), e);
        }

        // A producer with a newer schema sent a metric this service doesn't know; Avro mapped it to the enum's
        // default. Storing "UNKNOWN" would lose the real value, so dead-letter it and replay after upgrading.
        if (event.getMetric() == Metric.UNKNOWN) {
            throw new InvalidEventException("Unknown metric at " + where(record) + "; this service needs upgrading");
        }

        store.store(event, eventId, record.partition(), record.offset());
    }

    private static String where(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
