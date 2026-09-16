package dev.kangwoul.commerce.purchase;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PurchaseEventPublisherTest {
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, PurchaseEvent> template = mock(KafkaTemplate.class);
    private final PurchaseEvent event = new PurchaseEvent(UUID.randomUUID(), "c1", "옷", "서울",
            BigDecimal.TEN, Instant.now());

    @Test
    void waitsForBrokerAcknowledgementAndUsesRegionKey() throws Exception {
        CompletableFuture<SendResult<String, PurchaseEvent>> acknowledgement = new CompletableFuture<>();
        CountDownLatch sent = new CountDownLatch(1);
        when(template.send(PurchaseEventPublisher.TOPIC, event.region(), event)).thenAnswer(invocation -> {
            sent.countDown();
            return acknowledgement;
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var publish = executor.submit(() -> new PurchaseEventPublisher(template, Duration.ofSeconds(5)).publish(event));
            assertThat(sent.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(publish.isDone()).isFalse();
            acknowledgement.complete(null);
            publish.get(2, TimeUnit.SECONDS);
            verify(template).send(PurchaseEventPublisher.TOPIC, event.region(), event);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void asynchronousFailureDoesNotLookLikeSuccess() {
        when(template.send(anyString(), anyString(), eq(event)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("발행 실패")));
        assertThatThrownBy(() -> new PurchaseEventPublisher(template, Duration.ofSeconds(1)).publish(event))
                .isInstanceOf(PurchasePublishException.class);
    }

    @Test
    void timeoutDoesNotLookLikeSuccess() {
        when(template.send(anyString(), anyString(), eq(event))).thenReturn(new CompletableFuture<>());
        assertThatThrownBy(() -> new PurchaseEventPublisher(template, Duration.ofMillis(10)).publish(event))
                .isInstanceOf(PurchasePublishException.class)
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
    }

    @Test
    void synchronousKafkaFailureDoesNotLookLikeSuccess() {
        when(template.send(anyString(), anyString(), eq(event)))
                .thenThrow(new org.apache.kafka.common.KafkaException("전송 실패"));
        assertThatThrownBy(() -> new PurchaseEventPublisher(template, Duration.ofSeconds(1)).publish(event))
                .isInstanceOf(PurchasePublishException.class);
    }

    @Test
    void immediateTemplateFailureDoesNotLookLikeSuccess() {
        when(template.send(anyString(), anyString(), eq(event)))
                .thenThrow(new org.springframework.kafka.KafkaException("전송 실패"));
        assertThatThrownBy(() -> new PurchaseEventPublisher(template, Duration.ofSeconds(1)).publish(event))
                .isInstanceOf(PurchasePublishException.class);
    }

    @Test
    void interruptionPreservesThreadFlag() {
        when(template.send(anyString(), anyString(), eq(event))).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> new PurchaseEventPublisher(template, Duration.ofSeconds(1)).publish(event))
                    .isInstanceOf(PurchasePublishException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
