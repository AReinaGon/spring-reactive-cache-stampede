package com.example.cachestampede.repository;

import com.example.cachestampede.model.TicketAvailability;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Simulated reactive data source standing in for an R2DBC PostgreSQL repository.
 *
 * <p>A real R2DBC repository is wired in Phase 2; here the query is faked so the PoC and its load
 * test run without a live database. The {@link AtomicInteger} counts how many times the source is
 * actually executed, which is the metric the stampede test asserts on.
 *
 * <p>The increment lives inside {@link Mono#defer} so it fires once per <em>subscription</em>. That
 * is what makes Reactor's {@code .cache()} observable: a cached {@code Mono} is subscribed to its
 * source only once, so the counter rises by one no matter how many callers await it.
 */
@Repository
public class TicketRepository {

    private static final Logger log = LoggerFactory.getLogger(TicketRepository.class);

    /**
     * Simulated query latency. Kept wide on purpose: it must outlast the time it takes a burst to
     * fan out so that every concurrent caller is still in flight (and still missing the cache) when
     * the first query is issued — which is precisely the window a real TTL expiry opens under load.
     */
    private static final Duration SIMULATED_LATENCY = Duration.ofSeconds(1);

    private final AtomicInteger queryCount = new AtomicInteger(0);

    public Mono<TicketAvailability> findAvailabilityByEventId(String eventId) {
        return Mono.defer(() -> {
            int hit = queryCount.incrementAndGet();
            log.debug("DB HIT #{} -> eventId={}", hit, eventId);
            TicketAvailability availability = new TicketAvailability(
                    eventId,
                    "Concierto Black Friday Edition",
                    1500,
                    LocalDateTime.now());
            return Mono.just(availability);
        }).delayElement(SIMULATED_LATENCY);
    }

    public int getQueryCount() {
        return queryCount.get();
    }

    public void resetQueryCount() {
        queryCount.set(0);
    }
}
