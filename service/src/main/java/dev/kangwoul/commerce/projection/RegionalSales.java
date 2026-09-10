package dev.kangwoul.commerce.projection;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "regional_sales")
public class RegionalSales {
    @Id
    private String region;
    private long orderCount;
    private BigDecimal totalAmount;

    protected RegionalSales() {}

    public String getRegion() { return region; }
    public long getOrderCount() { return orderCount; }
    public BigDecimal getTotalAmount() { return totalAmount; }
}
