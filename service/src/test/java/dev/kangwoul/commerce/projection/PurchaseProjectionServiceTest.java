package dev.kangwoul.commerce.projection;

import dev.kangwoul.commerce.purchase.PurchaseEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PurchaseProjectionServiceTest {
    @Test
    void duplicateEventDoesNotIncrementProjectionTwice() {
        ProcessedEventRepository processed = mock(ProcessedEventRepository.class);
        RegionalSalesRepository sales = mock(RegionalSalesRepository.class);
        PurchaseProjectionService service = new PurchaseProjectionService(processed, sales);
        UUID id = UUID.randomUUID();
        PurchaseEvent event = new PurchaseEvent(id, "c1", "Outerwear", "Seoul", BigDecimal.TEN, Instant.now());
        when(processed.tryInsert(id)).thenReturn(0);

        boolean projected = service.project(event);

        assertThat(projected).isFalse();
        verify(sales, never()).increment("Seoul", BigDecimal.TEN);
    }
}
