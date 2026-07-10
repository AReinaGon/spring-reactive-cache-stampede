package com.areina.distributedlock.repository;

import com.areina.distributedlock.model.TicketAvailability;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Reactive R2DBC repository for ticket availability.
 *
 * <p>The {@link AtomicInteger} counts how many times the database is actually queried. In the
 * distributed test a single repository instance is shared by every simulated pod, so the counter
 * represents the load the database sees from the <em>whole cluster</em>: without coordination it
 * climbs to one query per pod, with the distributed lock it stays at exactly one. The increment
 * lives inside {@link Mono#defer} so it fires once per subscription.
 */
@Repository
public class TicketRepository {

    private static final Logger log = LoggerFactory.getLogger(TicketRepository.class);

    private final AtomicInteger queryCount = new AtomicInteger(0);
    private final DatabaseClient databaseClient;

    public TicketRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<TicketAvailability> findAvailabilityByEventId(String eventId) {
        return Mono.defer(() -> {
            int hit = queryCount.incrementAndGet();
            log.debug("DB HIT #{} -> eventId={}", hit, eventId);
            return databaseClient
                    .sql("SELECT event_id, event_name, available_seats FROM ticket_availability WHERE event_id = :eventId")
                    .bind("eventId", eventId)
                    .map(row -> new TicketAvailability(
                            row.get("event_id", String.class),
                            row.get("event_name", String.class),
                            Objects.requireNonNull(row.get("available_seats", Integer.class)),
                            LocalDateTime.now(),
                            null))
                    .one();
        });
    }

    public int getQueryCount() {
        return queryCount.get();
    }

    public void resetQueryCount() {
        queryCount.set(0);
    }
}
