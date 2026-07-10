package com.areina.distributedlock.service;

import com.areina.distributedlock.config.CacheProperties;
import com.areina.distributedlock.config.TicketJsonCodec;
import com.areina.distributedlock.model.TicketAvailability;
import com.areina.distributedlock.repository.TicketRepository;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RLockReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Stampede-safe distributed cache: a Redis-backed distributed lock funnels the whole pod cluster
 * through a single database query per expiry cycle.
 *
 * <p>The single-JVM trick from Phase 1 ("cache the in-flight {@code Mono}") cannot work here: a
 * {@code Mono} lives on one pod's heap and cannot be shared across JVMs. The distributed equivalent
 * is a {@code Lock -> double-check -> Fetch -> Cache -> Release} flow over a {@link RLockReactive}:
 *
 * <pre>{@code
 * Pod A (winner)        Redis            Pod B (loser)         Database
 *   |-- get(key) ------->|                  |                     |
 *   |<-- (empty) --------|                  |                     |
 *   |-- tryLock -------->|<-- tryLock ------|                     |
 *   |<-- acquired -------|-- denied/wait -->|                     |
 *   |-- get(key) [recheck, still empty] --->|                     |
 *   |-- query --------------------------------------------------->|
 *   |-- set(key, TTL) -->|                  |                     |
 *   |-- unlock --------->|-- lock granted ->|                     |
 *   |                    |<-- get(key) [recheck: HIT] ------------|
 *   |                    |-- value ------->| (no DB query)        |
 * }</pre>
 *
 * <p>Two details make the database hit count land on exactly one:
 *
 * <ul>
 *   <li><b>Double-checked read.</b> After acquiring the lock the pod reads Redis <em>again</em>;
 *       a loser that waited will now find the value the winner wrote and return it without touching
 *       the database. This is what collapses every loser into a cache hit.
 *   <li><b>Explicit {@code threadId}.</b> Reactor may run the acquire and the release on different
 *       scheduler threads, and a Redisson lock is owned by {@code (clientId, threadId)} — releasing
 *       from a different thread throws {@code IllegalMonitorStateException}. Each request mints its
 *       own stable id and passes it to both {@code tryLock} and {@code unlock} so ownership survives
 *       the thread hop.
 * </ul>
 *
 * <p>The pipeline never blocks: {@code .block()}, {@code synchronized} and {@code Thread.sleep()}
 * are all avoided; {@code tryLock}'s wait is implemented by Redisson over Redis pub/sub.
 */
@Service
public class TicketLockCacheService {

    private static final Logger log = LoggerFactory.getLogger(TicketLockCacheService.class);

    private static final String KEY_PREFIX = "tickets:availability:";
    private static final String LOCK_PREFIX = "lock:event:";

    /** Mints a distinct, stable lock-owner id per request (see class Javadoc). */
    private static final AtomicLong LOCK_OWNER_ID = new AtomicLong();

    /**
     * Counts how many requests actually enter the distributed-lock acquisition path. It is a teaching
     * instrument (like the repository's query counter): with no local layer every same-pod request
     * contends for the lock, so this climbs with intra-pod concurrency; a local promise cache in front
     * ({@code TicketLayeredCacheService}) collapses it to one attempt per pod.
     */
    private final AtomicInteger lockAttempts = new AtomicInteger(0);

    private final TicketRepository repository;
    private final RedissonReactiveClient redisson;
    private final TicketJsonCodec codec;
    private final CacheProperties cacheProperties;
    private final String podId;

    public TicketLockCacheService(
            TicketRepository repository,
            RedissonReactiveClient redisson,
            TicketJsonCodec codec,
            CacheProperties cacheProperties,
            @Value("${POD_NAME:#{T(java.util.UUID).randomUUID().toString().substring(0,8)}}") String podId) {
        this.repository = repository;
        this.redisson = redisson;
        this.codec = codec;
        this.cacheProperties = cacheProperties;
        this.podId = podId;
    }

    public Mono<TicketAvailability> getAvailability(String eventId) {
        RBucketReactive<String> bucket = redisson.getBucket(KEY_PREFIX + eventId, StringCodec.INSTANCE);
        return bucket.get()
                .doOnNext(v -> log.debug("[{}] Redis HIT eventId={}", podId, eventId))
                .map(codec::decode)
                .switchIfEmpty(Mono.defer(() -> lockAndLoad(eventId, bucket)));
    }

    private Mono<TicketAvailability> lockAndLoad(String eventId, RBucketReactive<String> bucket) {
        lockAttempts.incrementAndGet();
        long ownerId = LOCK_OWNER_ID.incrementAndGet();
        RLockReactive lock = redisson.getLock(LOCK_PREFIX + eventId);

        return Mono.usingWhen(
                acquire(lock, eventId, ownerId),
                acquiredLock -> bucket.get()
                        .doOnNext(v -> log.debug("[{}] re-check after lock: already cached, no DB hit", podId))
                        .map(codec::decode)
                        .switchIfEmpty(Mono.defer(() -> fetchAndCache(eventId, bucket))),
                // Release even on error; the lease may already have expired, so swallow a late unlock.
                acquiredLock -> acquiredLock.unlock(ownerId).onErrorResume(e -> Mono.empty()));
    }

    private Mono<RLockReactive> acquire(RLockReactive lock, String eventId, long ownerId) {
        return lock.tryLock(cacheProperties.lockWait().toMillis(), cacheProperties.lockLease().toMillis(),
                        TimeUnit.MILLISECONDS, ownerId)
                .flatMap(acquired -> Boolean.TRUE.equals(acquired)
                        ? Mono.just(lock)
                        : Mono.error(new IllegalStateException("[%s] could not acquire lock for %s within %s"
                                .formatted(podId, eventId, cacheProperties.lockWait()))));
    }

    private Mono<TicketAvailability> fetchAndCache(String eventId, RBucketReactive<String> bucket) {
        log.debug("[{}] lock acquired -> querying DB eventId={}", podId, eventId);
        return repository.findAvailabilityByEventId(eventId)
                .map(value -> value.handledBy(podId))
                .flatMap(value -> bucket.set(codec.encode(value), cacheProperties.valueTtl()).thenReturn(value));
    }

    /** Teaching instrument: number of requests that entered the distributed-lock acquisition path. */
    public int getLockAttemptCount() {
        return lockAttempts.get();
    }

    public void resetLockAttemptCount() {
        lockAttempts.set(0);
    }
}
