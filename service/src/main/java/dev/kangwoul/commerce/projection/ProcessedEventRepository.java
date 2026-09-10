package dev.kangwoul.commerce.projection;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
    @Modifying
    @Query(value = "INSERT INTO processed_events(event_id) VALUES (:eventId) ON CONFLICT DO NOTHING", nativeQuery = true)
    int tryInsert(@Param("eventId") UUID eventId);
}
