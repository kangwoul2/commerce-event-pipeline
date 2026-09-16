package dev.kangwoul.commerce.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;

@Configuration
public class KafkaConsumerConfig {
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        // DLQ가 없는 현재 단계에서는 실패 메시지를 건너뛰지 않고 소비를 멈춘다.
        // 원인을 해결한 뒤 애플리케이션을 재시작하면 미커밋 메시지를 다시 읽는다.
        return new CommonContainerStoppingErrorHandler();
    }
}
