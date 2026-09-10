package dev.kangwoul.commerce.config;

import dev.kangwoul.commerce.purchase.PurchaseEventPublisher;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    NewTopic purchaseEventsTopic() {
        return TopicBuilder.name(PurchaseEventPublisher.TOPIC).partitions(6).replicas(1).build();
    }
}
