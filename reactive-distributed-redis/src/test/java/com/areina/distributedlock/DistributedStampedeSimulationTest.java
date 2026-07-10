package com.areina.distributedlock;

import static org.assertj.core.api.Assertions.assertThat;

import com.areina.distributedlock.config.TicketJsonCodec;
import com.areina.distributedlock.model.TicketAvailability;
import com.areina.distributedlock.repository.TicketRepository;
import com.areina.distributedlock.service.TicketLayeredCacheService;
import com.areina.distributedlock.service.TicketLockCacheService;
import com.areina.distributedlock.service.TicketNoLockCacheService;
import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reproduces a <em>distributed</em> cache stampede against a real PostgreSQL + real Redis and proves
 * the distributed-lock mitigation.
 *
 * <p>A cluster of {@value #SIMULATED_PODS} pods is simulated inside one JVM: each "pod" is its own
 * service instance with a distinct id, but they all share the same Redis (one Redisson client) and
 * the same database counter — so the counter measures the load the database sees from the whole
 * cluster. Every pod fires a single request at the same cold key, all at once.
 *
 * <ul>
 *   <li>{@link TicketNoLockCacheService}: each pod misses Redis together and queries the database —
 *       roughly one query per pod.
 *   <li>{@link TicketLockCacheService}: a Redis distributed lock elects one pod to query the database;
 *       the rest re-read the now-populated key — exactly one query for the whole cluster.
 * </ul>
 *
 * <p>A third test isolates the <em>intra-pod</em> dimension: a single pod fires
 * {@value #REQUESTS_PER_POD} concurrent requests at the same cold key. The lock alone still lands on
 * one DB query, but every request contends for the lock; {@link TicketLayeredCacheService} adds a
 * local promise cache so only one request per pod ever reaches the lock.
 */
@SpringBootTest
@Testcontainers
class DistributedStampedeSimulationTest {

    private static final Logger log = LoggerFactory.getLogger(DistributedStampedeSimulationTest.class);

    private static final int SIMULATED_PODS = 20;
    private static final int REQUESTS_PER_POD = 50;
    private static final String EVENT_ID = "black-friday-2026";
    private static final String VALUE_KEY = "tickets:availability:" + EVENT_ID;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("ticketing_db")
            .withUsername("user_poc")
            .withPassword("pwd_poc")
            .withInitScript("db/schema.sql");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://%s:%d/%s".formatted(
                        postgres.getHost(),
                        postgres.getMappedPort(5432),
                        postgres.getDatabaseName()));
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
        registry.add("spring.r2dbc.pool.max-size", () -> "100");
        registry.add("spring.r2dbc.pool.initial-size", () -> "5");
        registry.add("redis.host", redis::getHost);
        registry.add("redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private TicketRepository repository;

    @Autowired
    private RedissonReactiveClient redisson;

    @Autowired
    private TicketJsonCodec ticketCodec;

    @BeforeEach
    void resetState() {
        repository.resetQueryCount();
        // Cold key for every scenario: clear any value left in Redis by a previous test.
        redisson.getBucket(VALUE_KEY, StringCodec.INSTANCE).delete().block();
    }

    @Test
    @DisplayName("No distributed lock: every pod misses Redis and stampedes the database")
    void noLockStampedesAcrossPods() {
        List<TicketNoLockCacheService> pods =
                pods(i -> new TicketNoLockCacheService(repository, redisson, ticketCodec, "pod-%02d".formatted(i)));

        long elapsedMs = fireOneRequestPerPod(pods, TicketNoLockCacheService::getAvailability);
        int dbQueries = repository.getQueryCount();

        log.info("[NO DISTRIBUTED LOCK] DB queries: {} | simulated pods: {} | total time: {}ms",
                dbQueries, SIMULATED_PODS, elapsedMs);

        assertThat(dbQueries)
                .as("without coordination every pod that misses the cold key queries the DB")
                .isGreaterThan(SIMULATED_PODS / 2);
    }

    @Test
    @DisplayName("Distributed lock: the whole pod cluster collapses into a single database query")
    void distributedLockCollapsesToSingleQuery() {
        List<TicketLockCacheService> pods =
                pods(i -> new TicketLockCacheService(repository, redisson, ticketCodec, "pod-%02d".formatted(i)));

        long elapsedMs = fireOneRequestPerPod(pods, TicketLockCacheService::getAvailability);
        int dbQueries = repository.getQueryCount();

        log.info("[DISTRIBUTED LOCK]    DB queries: {} | simulated pods: {} | total time: {}ms",
                dbQueries, SIMULATED_PODS, elapsedMs);

        assertThat(dbQueries)
                .as("the distributed lock funnels the whole cluster through one DB query")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("Local promise cache: same-pod requests share one in-flight Mono instead of each taking the lock")
    void localPromiseCacheCollapsesIntraPodLockContention() {
        // --- Distributed lock only: every same-pod request mints its own owner and contends for the lock. ---
        TicketLockCacheService lockOnly =
                new TicketLockCacheService(repository, redisson, ticketCodec, "pod-single");
        long lockOnlyMs = fireConcurrentRequests(() -> lockOnly.getAvailability(EVENT_ID));
        int lockOnlyDbQueries = repository.getQueryCount();
        int lockOnlyAttempts = lockOnly.getLockAttemptCount();

        // Cold start again for the second scenario.
        resetState();

        // --- Local promise cache in front of the same distributed lock. ---
        TicketLockCacheService delegate =
                new TicketLockCacheService(repository, redisson, ticketCodec, "pod-single");
        TicketLayeredCacheService layered = new TicketLayeredCacheService(delegate);
        long layeredMs = fireConcurrentRequests(() -> layered.getAvailability(EVENT_ID));
        int layeredDbQueries = repository.getQueryCount();
        int layeredAttempts = delegate.getLockAttemptCount();

        log.info("[LOCK ONLY]   requests: {} | DB queries: {} | lock attempts: {} | total time: {}ms",
                REQUESTS_PER_POD, lockOnlyDbQueries, lockOnlyAttempts, lockOnlyMs);
        log.info("[LOCAL+LOCK]  requests: {} | DB queries: {} | lock attempts: {} | total time: {}ms",
                REQUESTS_PER_POD, layeredDbQueries, layeredAttempts, layeredMs);

        // Both strategies keep the database at a single query...
        assertThat(lockOnlyDbQueries).isEqualTo(1);
        assertThat(layeredDbQueries).isEqualTo(1);
        // ...but the lock alone is hit by many same-pod requests, while the local cache collapses them to one.
        assertThat(lockOnlyAttempts)
                .as("without a local layer every same-pod request contends for the distributed lock")
                .isGreaterThan(1);
        assertThat(layeredAttempts)
                .as("the local promise cache funnels the whole same-pod burst through one lock attempt")
                .isEqualTo(1);
    }

    private <S> List<S> pods(IntFunction<S> makePod) {
        return IntStream.range(0, SIMULATED_PODS).mapToObj(makePod).toList();
    }

    /** Subscribes {@value #REQUESTS_PER_POD} requests at once from a single pod and blocks until all complete. */
    private long fireConcurrentRequests(Supplier<Mono<TicketAvailability>> call) {
        long t0 = System.nanoTime();
        Flux.range(0, REQUESTS_PER_POD)
                .flatMap(i -> call.get(), REQUESTS_PER_POD)
                .blockLast(Duration.ofSeconds(60));
        return (System.nanoTime() - t0) / 1_000_000;
    }

    /** Subscribes one request per pod simultaneously and blocks until all complete. */
    private <S> long fireOneRequestPerPod(List<S> pods, BiFunction<S, String, Mono<TicketAvailability>> call) {
        long t0 = System.nanoTime();
        Flux.fromIterable(pods)
                .flatMap(pod -> call.apply(pod, EVENT_ID), SIMULATED_PODS)
                .blockLast(Duration.ofSeconds(60));
        return (System.nanoTime() - t0) / 1_000_000;
    }
}
