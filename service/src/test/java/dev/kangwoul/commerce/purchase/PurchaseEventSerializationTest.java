package dev.kangwoul.commerce.purchase;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PurchaseEventSerializationTest {
    @Test
    @SuppressWarnings("unchecked")
    void configuredJsonConvertersPreserveEventFields() throws Exception {
        // 설정에 적힌 클래스를 직접 생성해야 JSON 라이브러리 버전 불일치도 잡을 수 있다.
        var yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        var properties = yaml.getObject();
        var event = new PurchaseEvent(UUID.randomUUID(), "고객1", "옷", "서울",
                new BigDecimal("12345.67"), Instant.parse("2026-09-17T00:00:00Z"));
        try (var serializer = (Serializer<PurchaseEvent>) Class.forName(
                properties.getProperty("spring.kafka.producer.value-serializer")).getConstructor().newInstance();
             var deserializer = (Deserializer<PurchaseEvent>) Class.forName(
                properties.getProperty("spring.kafka.consumer.value-deserializer")).getConstructor().newInstance()) {
            serializer.configure(Map.of(), false);
            deserializer.configure(Map.of("spring.json.trusted.packages",
                    properties.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages")), false);
            var headers = new RecordHeaders();
            byte[] payload = serializer.serialize("purchase-events", headers, event);
            assertThat(deserializer.deserialize("purchase-events", headers, payload)).isEqualTo(event);
        }
    }
}
