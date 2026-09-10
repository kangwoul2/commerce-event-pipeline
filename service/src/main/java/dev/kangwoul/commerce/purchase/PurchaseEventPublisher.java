package dev.kangwoul.commerce.purchase;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PurchaseEventPublisher {
    public static final String TOPIC = "purchase-events";
    private final KafkaTemplate<String, PurchaseEvent> kafkaTemplate;

    public PurchaseEventPublisher(KafkaTemplate<String, PurchaseEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(PurchaseEvent event) {
        // region is the partition key because the projection is aggregated by region.
        kafkaTemplate.send(TOPIC, event.region(), event);
    }
}
