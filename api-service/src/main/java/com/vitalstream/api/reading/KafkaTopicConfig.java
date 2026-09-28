package com.vitalstream.api.reading;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topics this service writes to. At startup Spring's KafkaAdmin creates any that are
 * missing, and (with spring.kafka.admin.modify-topic-configs) updates the settings of existing ones to
 * match, so the topic definition lives in code, not in a manual CLI step.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic readingsTopic(@Value("${vitalstream.kafka.topics.readings}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1) // single-broker dev cluster
                // Keep every event forever (the default is 7 days). The read models in vitals-query are
                // rebuilt by replaying this topic from the start, which only works if the start still exists.
                // The cost is disk space that grows without limit; the usual alternatives are tiered
                // storage (old segments moved to cheap object storage) or periodic read-model snapshots
                // plus a finite retention.
                .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
                .build();
    }
}
