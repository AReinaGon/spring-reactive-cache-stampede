package com.areina.distributedlock.controller;

import com.areina.distributedlock.model.TicketAvailability;
import com.areina.distributedlock.repository.TicketRepository;
import com.areina.distributedlock.service.TicketLayeredCacheService;
import com.areina.distributedlock.service.TicketLockCacheService;
import com.areina.distributedlock.service.TicketNoLockCacheService;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    private static final String VALUE_KEY_PREFIX = "tickets:availability:";

    private final TicketLockCacheService lockCacheService;
    private final TicketLayeredCacheService layeredCacheService;
    private final TicketNoLockCacheService noLockCacheService;
    private final RedissonReactiveClient redisson;

    public TicketController(
            TicketLockCacheService lockCacheService,
            TicketLayeredCacheService layeredCacheService,
            TicketNoLockCacheService noLockCacheService,
            RedissonReactiveClient redisson) {
        this.lockCacheService = lockCacheService;
        this.layeredCacheService = layeredCacheService;
        this.noLockCacheService = noLockCacheService;
        this.redisson = redisson;
    }

    /** Mitigated endpoint: the distributed lock collapses a cluster-wide burst into a single DB query. */
    @GetMapping("/availability/{eventId}")
    public Mono<TicketAvailability> getAvailability(@PathVariable String eventId) {
        return lockCacheService.getAvailability(eventId);
    }

    /**
     * Two-layer endpoint: a per-pod local promise cache in front of the distributed lock. Same single
     * DB query for the cluster, but same-pod concurrency is coalesced locally so only one request per
     * pod ever contends for the lock.
     */
    @GetMapping("/availability/{eventId}/layered")
    public Mono<TicketAvailability> getAvailabilityLayered(@PathVariable String eventId) {
        return layeredCacheService.getAvailability(eventId);
    }

    /** Naive endpoint: a Redis value cache with no coordination — stampedes the DB across pods. */
    @GetMapping("/availability/{eventId}/naive")
    public Mono<TicketAvailability> getAvailabilityNaive(@PathVariable String eventId) {
        return noLockCacheService.getAvailability(eventId);
    }

    /**
     * Cluster-wide DB-hit count, read from the shared Redis counter ({@link TicketRepository#DB_HITS_KEY}).
     * This is the load-test equivalent of the integration test's query counter: flush, fire the burst,
     * read this back. Without the lock it climbs to ~one hit per pod; with it, it stays at one.
     */
    @GetMapping("/stats")
    public Mono<Stats> stats() {
        return redisson.getAtomicLong(TicketRepository.DB_HITS_KEY).get().map(Stats::new);
    }

    /**
     * Forces a cold key for the next burst: deletes the cached value for {@code eventId} and resets the
     * cluster-wide DB-hit counter. Meant to be called from a JMeter setUp Thread Group before each run
     * (the in-JVM test does the same in its {@code @BeforeEach}).
     */
    @PostMapping("/cache/{eventId}/reset")
    public Mono<Stats> resetCache(@PathVariable String eventId) {
        Mono<Boolean> clearValue = redisson.getBucket(VALUE_KEY_PREFIX + eventId, StringCodec.INSTANCE).delete();
        Mono<Void> resetCounter = redisson.getAtomicLong(TicketRepository.DB_HITS_KEY).set(0L);
        return clearValue.then(resetCounter).thenReturn(new Stats(0L));
    }

    /** Cluster-wide DB-hit snapshot returned by {@code /stats} and {@code /cache/{eventId}/reset}. */
    public record Stats(long dbHits) {
    }
}
