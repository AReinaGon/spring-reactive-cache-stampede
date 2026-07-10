package com.areina.cachestampede.repository;

import com.areina.cachestampede.model.TicketAvailability;
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
 * <p>The {@link AtomicInteger} counts how many times the database is actually queried, which is the
 * metric the stampede test asserts on. The increment lives inside {@link Mono#defer} so it fires
 * once per <em>subscription</em> — that is what makes Reactor's {@code .cache()} observable: a
 * cached {@code Mono} is subscribed to its source only once, so the counter rises by one no matter
 * how many callers await it.
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
                            LocalDateTime.now()))
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
