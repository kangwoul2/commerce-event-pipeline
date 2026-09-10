package dev.kangwoul.commerce.purchase;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PurchaseEvent(
        UUID eventId,
        String customerId,
        String category,
        String region,
        BigDecimal amount,
        Instant occurredAt
) {}
