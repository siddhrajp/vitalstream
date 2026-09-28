package com.vitalstream.api.reading;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topics this service writes to. At startup Spring's KafkaAdmin creates any that are
 * missing and leaves existing ones alone, so the topic definition lives in code, not in a manual CLI step.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic readingsTopic(@Value("${vitalstream.kafka.topics.readings}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1) // single-broker dev cluster
                .build();
    }
}
