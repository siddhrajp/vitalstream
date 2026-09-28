package com.vitalstream.query.kafka;

import com.vitalstream.events.Metric;
import com.vitalstream.events.VitalReadingEvent;
import com.vitalstream.query.readmodel.ReadModelProjector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Feeds every reading event into the read models. Same topic as vitals-processor, different consumer
 * group (spring.kafka.consumer.group-id), so both services see every event independently.
 */
@Component
public class ReadingEventListener {

    private final ReadModelProjector projector;

    public ReadingEventListener(ReadModelProjector projector) {
        this.projector = projector;
    }

    @KafkaListener(topics = "${vitalstream.kafka.topics.readings}")
    public void onReading(ConsumerRecord<String, VitalReadingEvent> record) {
        VitalReadingEvent event = record.value();
        UUID eventId;
        try {
            eventId = UUID.fromString(event.getEventId());
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("eventId is not a UUID at " + where(record) + ": " + event.getEventId(), e);
        }
        if (event.getMetric() == Metric.UNKNOWN) {
            throw new InvalidEventException("Unknown metric at " + where(record) + "; this service needs upgrading");
        }
        projector.apply(event, eventId);
    }

    private static String where(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
