package dev.kangwoul.commerce.projection;

import dev.kangwoul.commerce.purchase.PurchaseEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PurchaseProjectionServiceTest {
    @Test
    void newEventIncrementsProjection() {
        ProcessedEventRepository processed = mock(ProcessedEventRepository.class);
        RegionalSalesRepository sales = mock(RegionalSalesRepository.class);
        PurchaseProjectionService service = new PurchaseProjectionService(processed, sales);
        PurchaseEvent event = new PurchaseEvent(UUID.randomUUID(), "c1", "Outerwear", "Seoul", BigDecimal.TEN, Instant.now());
        when(processed.tryInsert(event.eventId())).thenReturn(1);

        assertThat(service.project(event)).isTrue();
        verify(sales).increment("Seoul", BigDecimal.TEN);
    }

    @Test
    void databaseFailurePropagatesToConsumer() {
        ProcessedEventRepository processed = mock(ProcessedEventRepository.class);
        RegionalSalesRepository sales = mock(RegionalSalesRepository.class);
        PurchaseProjectionService service = new PurchaseProjectionService(processed, sales);
        PurchaseEvent event = new PurchaseEvent(UUID.randomUUID(), "c1", "Outerwear", "Seoul", BigDecimal.TEN, Instant.now());
        when(processed.tryInsert(event.eventId())).thenReturn(1);
        doThrow(new IllegalStateException("DB 갱신 실패")).when(sales).increment("Seoul", BigDecimal.TEN);

        // 가짜 저장소로는 롤백을 검증할 수 없다. 여기서는 예외를 삼키지 않는지만 확인한다.
        assertThatThrownBy(() -> service.project(event)).isInstanceOf(IllegalStateException.class);
    }

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
