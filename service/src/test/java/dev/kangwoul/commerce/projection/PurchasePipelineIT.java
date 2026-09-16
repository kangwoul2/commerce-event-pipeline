package dev.kangwoul.commerce.projection;

import dev.kangwoul.commerce.purchase.PurchaseEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

// docker compose up -d 후 mvn -Pintegration verify로 실행한다.
// 테스트 전체에 @Transactional을 붙이지 않는다. 실제 서비스의 커밋·롤백을 관찰해야 한다.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PurchasePipelineIT {
    @Autowired PurchaseProjectionService projection;
    @Autowired ProcessedEventRepository processed;
    @Autowired RegionalSalesRepository sales;
    @Autowired JdbcTemplate jdbc;
    @LocalServerPort int port;

    private PurchaseEvent event(UUID id, String region) {
        return new PurchaseEvent(id, "실험용-고객", "옷", region, new BigDecimal("10.25"), Instant.now());
    }

    @Test
    void repeatedIdChangesRealDatabaseOnlyOnce() {
        var event = event(UUID.randomUUID(), "중복-" + UUID.randomUUID());
        assertThat(projection.project(event)).isTrue();
        assertThat(projection.project(event)).isFalse();
        assertSales(event.region(), 1, "10.25");
    }

    @Test
    void failedUpdateRollsBackIdAndAllowsRetry() {
        UUID id = UUID.randomUUID();
        // API를 거치지 않고 DB 컬럼 길이를 초과시켜 ID 삽입 이후의 실패를 재현한다.
        assertThatThrownBy(() -> projection.project(event(id, "가".repeat(121))))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(processed.existsById(id)).isFalse();
        String region = "재시도-" + id;
        assertThat(projection.project(event(id, region))).isTrue();
        assertSales(region, 1, "10.25");
    }

    @Test
    void concurrentCopiesOfSameIdAreAppliedOnce() throws Exception {
        var event = event(UUID.randomUUID(), "동시중복-" + UUID.randomUUID());
        var results = concurrently(() -> projection.project(event));
        assertThat(results.stream().filter(Boolean::booleanValue).count()).isEqualTo(1);
        assertSales(event.region(), 1, "10.25");
    }

    @Test
    void concurrentDistinctEventsDoNotLoseUpdates() throws Exception {
        String region = "동시증가-" + UUID.randomUUID();
        assertThat(concurrently(() -> projection.project(event(UUID.randomUUID(), region))))
                .containsOnly(true);
        assertSales(region, 24, "246.00");
    }

    @Test
    void httpKafkaAndDatabaseApplyRepeatedRequestOnce() throws Exception {
        UUID id = UUID.randomUUID();
        String region = "전체경로-" + id;
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/purchases"))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json").header("Idempotency-Key", id.toString())
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"customerId":"실험용-고객","category":"옷","region":"%s","amount":10.25}
                        """.formatted(region))).build();
        for (int i = 0; i < 5; i++) {
            assertThat(client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(202);
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(processed.existsById(id)).isTrue();
            assertSales(region, 1, "10.25");
        });
        // 첫 반영만 보고 끝내지 않고 뒤따르는 중복 메시지가 소비될 시간을 둔다.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertSales(region, 1, "10.25"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Long.class, id))
                .isEqualTo(1L);
        HttpRequest query = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/analytics/regions"))
                .timeout(Duration.ofSeconds(5)).GET().build();
        var response = client.send(query, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains(region);
    }

    @Test
    void experimentScriptVerifiesRunningPipeline() throws Exception {
        var builder = new ProcessBuilder("python", "../experiments/replay_duplicate.py",
                "--base-url", "http://localhost:" + port, "--repeat", "5");
        builder.environment().put("PYTHONUTF8", "1");
        var process = builder.redirectErrorStream(true).start();
        try {
            assertThat(process.waitFor(60, TimeUnit.SECONDS)).isTrue();
            String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertThat(process.exitValue()).withFailMessage(output).isZero();
            assertThat(output).contains("검증 성공");
        } finally {
            process.destroy();
        }
    }

    private java.util.List<Boolean> concurrently(Callable<Boolean> operation) throws Exception {
        var executor = Executors.newFixedThreadPool(8);
        var ready = new CountDownLatch(8);
        var start = new CountDownLatch(1);
        try {
            var tasks = new ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < 24; i++) {
                tasks.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("동시 실행 시작 실패");
                    return operation.call();
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var results = new ArrayList<Boolean>();
            for (var task : tasks) results.add(task.get(20, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    private void assertSales(String region, long count, String amount) {
        var stored = sales.findById(region);
        assertThat(stored).isPresent();
        RegionalSales result = stored.orElseThrow();
        assertThat(result.getOrderCount()).isEqualTo(count);
        assertThat(result.getTotalAmount()).isEqualByComparingTo(amount);
    }
}
