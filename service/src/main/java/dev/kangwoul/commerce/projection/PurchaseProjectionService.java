package dev.kangwoul.commerce.projection;

import dev.kangwoul.commerce.purchase.PurchaseEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PurchaseProjectionService {
    private final ProcessedEventRepository processedEvents;
    private final RegionalSalesRepository regionalSales;

    public PurchaseProjectionService(ProcessedEventRepository processedEvents, RegionalSalesRepository regionalSales) {
        this.processedEvents = processedEvents;
        this.regionalSales = regionalSales;
    }

    @Transactional
    public boolean project(PurchaseEvent event) {
        int inserted = processedEvents.tryInsert(event.eventId());
        if (inserted == 0) {
            return false;
        }
        regionalSales.increment(event.region(), event.amount());
        return true;
    }
}
