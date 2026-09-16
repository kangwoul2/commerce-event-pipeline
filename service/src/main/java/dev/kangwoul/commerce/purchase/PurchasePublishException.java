package dev.kangwoul.commerce.purchase;

public class PurchasePublishException extends RuntimeException {
    public PurchasePublishException(Throwable cause) {
        super("구매 이벤트 발행 결과를 확인하지 못했습니다.", cause);
    }
}
