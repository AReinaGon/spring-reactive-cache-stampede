package com.example.cachestampede;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cachestampede.model.TicketAvailability;
import com.example.cachestampede.repository.TicketRepository;
import com.example.cachestampede.service.TicketPromiseCacheService;
import com.example.cachestampede.service.TicketValueCacheService;
import com.github.benmanes.caffeine.cache.Cache;
import java.time.Duration;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Empirically reproduces a cache stampede and proves the reactive mitigation.
 *
 * <p>Each test fires 2,000 fully concurrent requests at one service against the same key
 * ({@code Flux.range(0, N).flatMap(..., N)} subscribes to all 2,000 inner publishers before any of
 * them completes) and counts how many times the simulated database is actually queried.
 *
 * <ul>
 *   <li>{@link TicketValueCacheService} stores the resolved value, so the whole burst misses the
 *       cold key together and stampedes the database — close to 2,000 queries.
 *   <li>{@link TicketPromiseCacheService} stores the in-flight {@code Mono}, so the burst shares one
 *       promise and the database is queried exactly once.
 * </ul>
 *
 * <p>The concurrency is driven at the service layer (not over HTTP) so the result measures the
 * caching strategy itself rather than the connection limits of a load-test client.
 */
@SpringBootTest
class StampedeSimulationTest {

    private static final Logger log = LoggerFactory.getLogger(StampedeSimulationTest.class);

    private static final int CONCURRENT_REQUESTS = 2_000;
    private static final String EVENT_ID = "black-friday-2026";

    @Autowired
    private TicketValueCacheService valueCacheService;

    @Autowired
    private TicketPromiseCacheService promiseCacheService;

    @Autowired
    private TicketRepository repository;

    @Autowired
    @Qualifier("valueCache")
    private Cache<String, TicketAvailability> valueCache;

    @Autowired
    @Qualifier("promiseCache")
    private Cache<String, Mono<TicketAvailability>> promiseCache;

    @BeforeEach
    void resetState() {
        repository.resetQueryCount();
        valueCache.invalidateAll();
        promiseCache.invalidateAll();
    }

    @Test
    @DisplayName("Naive value cache: a 2,000-request burst on a cold key stampedes the database")
    void naiveValueCacheStampedesTheDatabase() {
        long elapsedMs = fireConcurrentBurst(valueCacheService::getAvailability);
        int dbQueries = repository.getQueryCount();

        log.info("[NAIVE VALUE CACHE]  DB queries: {} | total time: {}ms", dbQueries, elapsedMs);

        assertThat(dbQueries)
                .as("naive value cache lets the cold-key burst stampede the DB (approaches the request count)")
                .isGreaterThan(CONCURRENT_REQUESTS * 3 / 4);
    }

    @Test
    @DisplayName("Reactive promise cache: the same 2,000-request burst collapses into a single query")
    void reactivePromiseCacheCollapsesToSingleQuery() {
        long elapsedMs = fireConcurrentBurst(promiseCacheService::getAvailability);
        int dbQueries = repository.getQueryCount();

        log.info("[REACTIVE PROMISE]   DB queries: {} | total time: {}ms", dbQueries, elapsedMs);

        assertThat(dbQueries)
                .as("reactive promise cache collapses the burst into a single query")
                .isEqualTo(1);
    }

    /**
     * Subscribes to {@code CONCURRENT_REQUESTS} calls at once and blocks until all complete,
     * returning the elapsed wall-clock time in milliseconds.
     */
    private long fireConcurrentBurst(Function<String, Mono<TicketAvailability>> call) {
        long t0 = System.nanoTime();
        Flux.range(0, CONCURRENT_REQUESTS)
                .flatMap(i -> call.apply(EVENT_ID), CONCURRENT_REQUESTS)
                .blockLast(Duration.ofSeconds(60));
        return (System.nanoTime() - t0) / 1_000_000;
    }
}
