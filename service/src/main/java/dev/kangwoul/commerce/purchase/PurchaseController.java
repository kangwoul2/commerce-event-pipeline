package dev.kangwoul.commerce.purchase;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/purchases")
public class PurchaseController {
    private final PurchaseEventPublisher publisher;

    public PurchaseController(PurchaseEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> purchase(
            @RequestHeader("Idempotency-Key") UUID eventId,
            @Valid @RequestBody PurchaseRequest request) {
        PurchaseEvent event = new PurchaseEvent(
                eventId,
                request.customerId(),
                request.category(),
                request.region(),
                request.amount(),
                Instant.now());
        publisher.publish(event);
        return ResponseEntity.accepted().body(Map.of("eventId", eventId, "status", "ACCEPTED"));
    }
}
