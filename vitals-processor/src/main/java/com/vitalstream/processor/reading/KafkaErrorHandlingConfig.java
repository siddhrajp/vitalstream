package com.vitalstream.processor.reading;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * What happens when ReadingListener throws.
 *
 * Two kinds of failure need opposite treatment:
 *  - Permanent (InvalidEventException: bad JSON, missing fields): retrying can never help, so the
 *    message goes straight to the dead-letter topic and the partition moves on immediately.
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
     * topic needs at least as many partitions as vitals.readings.
     */
    @Bean
    public NewTopic readingsDeadLetterTopic() {
        return TopicBuilder.name(deadLetterTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /** Spring Boot plugs this bean into every @KafkaListener automatically. */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // The destination is set explicitly rather than left to Spring's naming default, so the topic
        // created above and the topic written to here can't drift apart.
        DeadLetterPublishingRecoverer deadLetter = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(deadLetterTopic, record.partition()));

        // Waits of 1s, 2s, 4s, 8s, 10s, 10s between attempts (about 35s in total), then dead-letter.
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(6);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetter, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Attempt {} failed for {}-{}@{}: {}", attempt, record.topic(), record.partition(),
                        record.offset(), NestedExceptionUtils.getMostSpecificCause(ex).getMessage()));
        return handler;
    }
}
