package dev.kangwoul.commerce.projection;

import dev.kangwoul.commerce.purchase.PurchaseEvent;
import dev.kangwoul.commerce.purchase.PurchaseEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PurchaseEventConsumer {
    private final PurchaseProjectionService projectionService;

    public PurchaseEventConsumer(PurchaseProjectionService projectionService) {
        this.projectionService = projectionService;
    }

    @KafkaListener(topics = PurchaseEventPublisher.TOPIC, groupId = "regional-sales-projection")
    public void consume(PurchaseEvent event) {
        projectionService.project(event);
    }
}
