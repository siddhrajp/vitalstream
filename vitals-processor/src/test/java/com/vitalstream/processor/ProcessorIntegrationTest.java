package com.vitalstream.processor;

import com.vitalstream.events.Metric;
import com.vitalstream.events.VitalReadingEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * vitals-processor against a real PostgreSQL and Kafka (Testcontainers; Docker must be running), with the
 * Avro serializers' in-memory schema registry. The test plays api-service: it publishes events to
 * vitals.readings.avro and checks what the processor stores or dead-letters.
 */
@SpringBootTest(properties = {
        "spring.kafka.properties.schema.registry.url=" + ProcessorIntegrationTest.MOCK_REGISTRY,
        "server.port=0"
})
@Testcontainers
class ProcessorIntegrationTest {

    static final String MOCK_REGISTRY = "mock://processor-it";
    static final String TOPIC = "vitals.readings.avro";

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void eventIsStoredExactlyOnceEvenIfDeliveredTwice() {
        VitalReadingEvent event = event(UUID.randomUUID().toString(), "service-account-test");
        publish(event);
        publish(event);   // same event id again, as a retrying producer would send it

        awaitTrue("the reading is stored", () -> rows(event.getEventId()) == 1);
        // Give the duplicate time to be processed, then check it didn't create a second row.
        sleep(2_000);
        assertThat(rows(event.getEventId())).isEqualTo(1);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT device_id, metric, value, submitted_by FROM vitals.vital_readings WHERE event_id = ?::uuid",
                event.getEventId());
        assertThat(row).containsEntry("device_id", 42L).containsEntry("metric", "HEART_RATE")
                .containsEntry("value", 72.0).containsEntry("submitted_by", "service-account-test");
    }

    @Test
    void invalidEventGoesToTheDeadLetterTopicAndIsNotStored() {
        VitalReadingEvent bad = event("not-a-uuid", "dlt-check");   // marker to look for in the table below
        publish(bad);

        String deadLettered = readDeadLetter("not-a-uuid");
        assertThat(deadLettered).contains("not-a-uuid");
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM vitals.vital_readings WHERE device_id = 42 AND submitted_by = 'dlt-check'",
                Long.class)).isZero();
    }

    // ---------- helpers ----------

    private static VitalReadingEvent event(String eventId, String submittedBy) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return VitalReadingEvent.newBuilder()
                .setEventId(eventId).setDeviceId(42L).setPatientId(7L).setMetric(Metric.HEART_RATE).setValue(72.0)
                .setMeasuredAt(now).setReceivedAt(now).setSubmittedBy(submittedBy)
                .build();
    }

    private static void publish(VitalReadingEvent event) {
        Map<String, Object> config = Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "schema.registry.url", MOCK_REGISTRY);
        KafkaAvroSerializer avro = new KafkaAvroSerializer();
        avro.configure(config, false);
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(config, new StringSerializer(), avro)) {
            producer.send(new ProducerRecord<>(TOPIC, String.valueOf(event.getDeviceId()), event));
            producer.flush();
        }
    }

    private long rows(String eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM vitals.vital_readings WHERE event_id::text = ?",
                Long.class, eventId);
    }

    /** Reads the processor's dead-letter topic until a message containing `marker` appears. */
    private static String readDeadLetter(String marker) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of("vitals.readings.avro.dlt"));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                    // Avro binary still contains strings as plain UTF-8 bytes, so the marker is findable.
                    String text = new String(record.value(), StandardCharsets.UTF_8);
                    if (text.contains(marker)) {
                        return text;
                    }
                }
            }
        }
        throw new AssertionError("Nothing containing '" + marker + "' on the dead-letter topic within 30 s");
    }

    private static void awaitTrue(String what, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(250);
        }
        throw new AssertionError("Timed out waiting until " + what);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
