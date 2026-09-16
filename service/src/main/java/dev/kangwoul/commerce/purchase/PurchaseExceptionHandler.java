package dev.kangwoul.commerce.purchase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class PurchaseExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(PurchaseExceptionHandler.class);

    @ExceptionHandler(PurchasePublishException.class)
    public ProblemDetail publishFailed(PurchasePublishException exception) {
        log.warn("구매 이벤트 발행 확인 실패", exception);
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "발행 결과를 확인하지 못했습니다. 같은 Idempotency-Key와 본문으로 재시도해 주세요.");
    }
}
