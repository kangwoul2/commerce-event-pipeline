package dev.kangwoul.commerce.projection;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsController {
    private final RegionalSalesRepository repository;

    public AnalyticsController(RegionalSalesRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/regions")
    public List<RegionalSales> regions() {
        return repository.findAll();
    }
}
