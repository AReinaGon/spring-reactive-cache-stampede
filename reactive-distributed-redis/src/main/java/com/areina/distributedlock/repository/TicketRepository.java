package com.areina.distributedlock.repository;

import com.areina.distributedlock.model.TicketAvailability;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.redisson.api.RedissonReactiveClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Reactive R2DBC repository for ticket availability, instrumented with two complementary DB-hit
 * counters because the PoC runs in two very different shapes.
 *
 * <ul>
 *   <li><b>Local {@link AtomicInteger} ({@code queryCount}).</b> Used by the in-JVM simulation test,
 *       where one repository instance is shared by every simulated pod — so this single counter is
 *       already the whole-cluster figure.
 *   <li><b>Shared Redis counter ({@code metrics:db-hits}).</b> Once the app is deployed as real,
 *       separate pods (Docker/Kubernetes), each JVM has its own {@code queryCount} and none of them
 *       sees the cluster total. Every real DB query therefore also does a Redis {@code INCR} on a
 *       shared key, which the {@code /api/tickets/stats} endpoint reads back. That is what makes a
 *       JMeter run across many pods measurable the same way the test is: ~one hit per pod without the
 *       lock, exactly one for the whole cluster with it.
 * </ul>
 *
 * <p>Both increments live inside {@link Mono#defer}/the reactive chain so they fire once per actual
 * subscription (i.e. per real query), never on a Redis cache hit.
 */
@Repository
public class TicketRepository {

    private static final Logger log = LoggerFactory.getLogger(TicketRepository.class);

    /** Shared, cluster-wide DB-hit counter key (read by {@code GET /api/tickets/stats}). */
    public static final String DB_HITS_KEY = "metrics:db-hits";

    private final AtomicInteger queryCount = new AtomicInteger(0);
    private final DatabaseClient databaseClient;
    private final RedissonReactiveClient redisson;

    public TicketRepository(DatabaseClient databaseClient, RedissonReactiveClient redisson) {
        this.databaseClient = databaseClient;
        this.redisson = redisson;
    }

    public Mono<TicketAvailability> findAvailabilityByEventId(String eventId) {
        return Mono.defer(() -> {
            int hit = queryCount.incrementAndGet();
            log.debug("DB HIT #{} -> eventId={}", hit, eventId);
            return databaseClient
                    .sql("SELECT event_id, event_name, available_seats, pg_sleep(0.5) FROM ticket_availability WHERE event_id = :eventId")
                    .bind("eventId", eventId)
                    .map(row -> new TicketAvailability(
                            row.get("event_id", String.class),
                            row.get("event_name", String.class),
                            Objects.requireNonNull(row.get("available_seats", Integer.class)),
                            LocalDateTime.now(),
                            null))
                    .one()
                    // Record the cluster-wide hit too, so a real multi-pod run is observable via /stats.
                    .flatMap(value -> redisson.getAtomicLong(DB_HITS_KEY).incrementAndGet().thenReturn(value));
        });
    }

    public int getQueryCount() {
        return queryCount.get();
    }

    public void resetQueryCount() {
        queryCount.set(0);
    }
}
