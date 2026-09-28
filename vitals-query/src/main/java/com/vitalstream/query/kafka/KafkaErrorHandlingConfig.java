package com.vitalstream.query.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What happens when a record can't be deserialized or ReadingEventListener throws.
 *
 * Two kinds of failure need opposite treatment:
 *  - Permanent (not valid Avro, InvalidEventException): retrying can never help, so the message goes
 *    straight to the dead-letter topic and the partition moves on immediately.
 *  - Transient (database down, network blip): retry with growing pauses, because it will probably
 *    work in a few seconds. If it still fails after all retries, it goes to the dead-letter topic too.
 *
 * Nothing is dropped silently: a dead-lettered message keeps its original key and value, plus headers
 * saying which topic/partition/offset it came from and which exception sent it there, so it can be
 * inspected and replayed later.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    private final String deadLetterTopic;

    public KafkaErrorHandlingConfig(@Value("${vitalstream.kafka.topics.readings-dead-letter}") String deadLetterTopic) {
        this.deadLetterTopic = deadLetterTopic;
    }

    /**
     * Failed records are written to the same partition number they came from, so the dead-letter
     * topic needs at least as many partitions as vitals.readings.avro.
     */
    @Bean
    public NewTopic readingsDeadLetterTopic() {
        return TopicBuilder.name(deadLetterTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Producer used only for dead-lettering. A failed record's value is one of two things:
     *  - byte[]: the record never became an object (not valid Avro), so its original bytes are forwarded as-is.
     *  - an Avro object: it deserialized fine but processing failed, so it's written back out as Avro.
     * DelegatingByTypeSerializer picks the right serializer for each value's type.
     */
    @Bean
    public KafkaTemplate<String, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties(null);

        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(config, false); // false = this serializes values, not keys

        Map<Class<?>, Serializer<?>> byType = new LinkedHashMap<>();
        byType.put(byte[].class, new ByteArraySerializer());
        byType.put(SpecificRecord.class, avroSerializer);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config,
                new StringSerializer(), new DelegatingByTypeSerializer(byType, true)));
    }

    /** Spring Boot plugs this bean into every @KafkaListener automatically. */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> deadLetterKafkaTemplate) {
        // The destination is set explicitly rather than left to Spring's naming default, so the topic
        // created above and the topic written to here can't drift apart.
        DeadLetterPublishingRecoverer deadLetter = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate,
                (record, ex) -> new TopicPartition(deadLetterTopic, record.partition()));

        // Waits of 1s, 2s, 4s, 8s, 10s, 10s between attempts (about 35s in total), then dead-letter.
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(6);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        // DeserializationException (from ErrorHandlingDeserializer) is already not retryable by default.
        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetter, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Attempt {} failed for {}-{}@{}: {}", attempt, record.topic(), record.partition(),
                        record.offset(), NestedExceptionUtils.getMostSpecificCause(ex).getMessage()));
        return handler;
    }
}
