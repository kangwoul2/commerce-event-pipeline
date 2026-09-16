package dev.kangwoul.commerce.purchase;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class PurchaseEventPublisher {
    public static final String TOPIC = "purchase-events";
    private final KafkaTemplate<String, PurchaseEvent> kafkaTemplate;
    private final Duration publishTimeout;

    public PurchaseEventPublisher(KafkaTemplate<String, PurchaseEvent> kafkaTemplate,
            @Value("${app.kafka.publish-timeout:10s}") Duration publishTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.publishTimeout = publishTimeout;
    }

    public void publish(PurchaseEvent event) {
        try {
            // 집계 완료가 아니라 Kafka 발행 성공까지 확인한다. 지역은 파티션 키다.
            kafkaTemplate.send(TOPIC, event.region(), event)
                    .get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PurchasePublishException(e);
        } catch (ExecutionException | TimeoutException | org.apache.kafka.common.KafkaException
                 | org.springframework.kafka.KafkaException e) {
            // 시간 초과 시 실제 발행 여부가 불확실하므로 재요청에서도 같은 ID를 사용해야 한다.
            throw new PurchasePublishException(e);
        }
    }
}
