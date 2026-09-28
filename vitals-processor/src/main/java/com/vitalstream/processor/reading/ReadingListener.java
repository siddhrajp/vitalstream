package com.vitalstream.processor.reading;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReadingListener {

    private final ObjectMapper json;
    private final ReadingStore store;

    public ReadingListener(ObjectMapper json, ReadingStore store) {
        this.json = json;
        this.store = store;
    }

    /**
     * Spring runs a loop per listener thread: poll Kafka for a batch of records, call this method once per
     * record, then commit the offsets of the batch. If this method throws, the offset is not committed and
     * Spring's error handler decides what to do (by default: retry the record 9 more times, then log and skip it).
     */
    @KafkaListener(topics = "${vitalstream.kafka.topics.readings}")
    public void onReading(ConsumerRecord<String, String> record) {
        VitalReadingEvent event;
        try {
            event = json.readValue(record.value(), VitalReadingEvent.class);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Not valid JSON at " + where(record), e);
        }
        if (!event.isComplete()) {
            throw new InvalidEventException("Missing required fields at " + where(record) + ": " + record.value());
        }
        store.store(event, record.partition(), record.offset());
    }

    private static String where(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
