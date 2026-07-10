package com.areina.cachestampede.repository;

import com.areina.cachestampede.model.TicketAvailability;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
 *
 * <p><b>Load-test knob.</b> {@code loadtest.query-latency} (default {@code 0s}) makes the query
 * simulate an expensive downstream call by holding the connection with {@code pg_sleep}. Because
 * the sleep runs inside the SQL, the R2DBC connection stays checked out for its whole duration —
 * so a stampede of concurrent queries drains a small connection pool and starts timing out, which
 * is how the "the database falls over" scenario is reproduced. Left at {@code 0s} the query is the
 * plain fast lookup used everywhere else.
 */
@Repository
public class TicketRepository {

    private static final Logger log = LoggerFactory.getLogger(TicketRepository.class);

    private static final String FAST_SQL =
            "SELECT event_id, event_name, available_seats FROM ticket_availability WHERE event_id = :eventId";
    // pg_sleep(:latency) is cross-joined so the connection is held for the sleep before rows return.
    private static final String SLOW_SQL =
            "SELECT t.event_id, t.event_name, t.available_seats "
                    + "FROM ticket_availability t, pg_sleep(:latency) WHERE t.event_id = :eventId";

    private final AtomicInteger queryCount = new AtomicInteger(0);
    private final DatabaseClient databaseClient;
    private final double queryLatencySeconds;
    private final boolean slowQuery;

    public TicketRepository(
            DatabaseClient databaseClient,
            @Value("${loadtest.query-latency:0s}") Duration queryLatency) {
        this.databaseClient = databaseClient;
        this.queryLatencySeconds = queryLatency.toMillis() / 1000.0;
        this.slowQuery = queryLatencySeconds > 0;
        if (slowQuery) {
            log.info("Load-test mode: simulating an expensive downstream with pg_sleep({}s) per query",
                    queryLatencySeconds);
        }
    }

    public Mono<TicketAvailability> findAvailabilityByEventId(String eventId) {
        return Mono.defer(() -> {
            int hit = queryCount.incrementAndGet();
            log.debug("DB HIT #{} -> eventId={}", hit, eventId);
            DatabaseClient.GenericExecuteSpec spec =
                    databaseClient.sql(slowQuery ? SLOW_SQL : FAST_SQL).bind("eventId", eventId);
            if (slowQuery) {
                spec = spec.bind("latency", queryLatencySeconds);
            }
            return spec
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
