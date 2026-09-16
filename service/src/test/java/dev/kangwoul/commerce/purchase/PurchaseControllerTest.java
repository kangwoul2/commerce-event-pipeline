package dev.kangwoul.commerce.purchase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class PurchaseControllerTest {
    private PurchaseEventPublisher publisher;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        publisher = mock(PurchaseEventPublisher.class);
        mvc = MockMvcBuilders.standaloneSetup(new PurchaseController(publisher))
                .setControllerAdvice(new PurchaseExceptionHandler()).build();
    }

    private String body(String region, String amount) {
        return """
                {"customerId":"c1","category":"옷","region":"%s","amount":%s}
                """.formatted(region, amount);
    }

    @Test
    void acceptedResponseKeepsClientEventId() throws Exception {
        String id = UUID.randomUUID().toString();
        mvc.perform(post("/api/v1/purchases").header("Idempotency-Key", id)
                        .contentType(MediaType.APPLICATION_JSON).content(body("서울", "10000")))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.eventId").value(id));
        verify(publisher).publish(argThat(event -> event.eventId().toString().equals(id)
                && event.region().equals("서울")));
    }

    @Test
    void publishFailureReturns503() throws Exception {
        doThrow(new PurchasePublishException(new IllegalStateException("발행 실패")))
                .when(publisher).publish(any());
        mvc.perform(post("/api/v1/purchases").header("Idempotency-Key", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(body("서울", "10000")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("같은 Idempotency-Key")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "1.001", "1000000000000000000", "null"})
    void invalidAmountIsRejectedBeforePublishing(String amount) throws Exception {
        mvc.perform(post("/api/v1/purchases").header("Idempotency-Key", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(body("서울", amount)))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(publisher);
    }

    @Test
    void oversizedRegionIsRejectedBeforePublishing() throws Exception {
        mvc.perform(post("/api/v1/purchases").header("Idempotency-Key", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(body("가".repeat(121), "10")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(publisher);
    }

    @Test
    void missingKeyIsRejected() throws Exception {
        mvc.perform(post("/api/v1/purchases").contentType(MediaType.APPLICATION_JSON).content(body("서울", "10")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(publisher);
    }

    @Test
    void malformedKeyIsRejected() throws Exception {
        mvc.perform(post("/api/v1/purchases").header("Idempotency-Key", "invalid")
                        .contentType(MediaType.APPLICATION_JSON).content(body("서울", "10")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(publisher);
    }
}
