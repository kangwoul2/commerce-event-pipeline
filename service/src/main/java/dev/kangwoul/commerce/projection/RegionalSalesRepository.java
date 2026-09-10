package dev.kangwoul.commerce.projection;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

public interface RegionalSalesRepository extends JpaRepository<RegionalSales, String> {
    @Modifying
    @Query(value = """
        INSERT INTO regional_sales(region, order_count, total_amount)
        VALUES (:region, 1, :amount)
        ON CONFLICT (region) DO UPDATE
        SET order_count = regional_sales.order_count + 1,
            total_amount = regional_sales.total_amount + EXCLUDED.total_amount
        """, nativeQuery = true)
    int increment(@Param("region") String region, @Param("amount") BigDecimal amount);
}
