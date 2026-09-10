package dev.kangwoul.commerce.purchase;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record PurchaseRequest(
        @NotBlank String customerId,
        @NotBlank String category,
        @NotBlank String region,
        @NotNull @DecimalMin("0.01") BigDecimal amount
) {}
