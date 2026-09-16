package dev.kangwoul.commerce.purchase;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record PurchaseRequest(
        @NotBlank String customerId,
        @NotBlank String category,
        @NotBlank @Size(max = 120) String region,
        @NotNull @DecimalMin("0.01") @Digits(integer = 18, fraction = 2) BigDecimal amount
) {}
