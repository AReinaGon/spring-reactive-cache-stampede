package com.areina.distributedlock.service;

import com.areina.distributedlock.model.TicketAvailability;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Two complementary layers against the cache stampede: a <b>per-pod local promise cache</b> in front
 * of the <b>cluster-wide distributed lock</b> ({@link TicketLockCacheService}).
 *
 * <p>The distributed lock already guarantees a single database query for the whole cluster, so this
 * layer is <em>not required for correctness</em>. What it removes is the waste the lock leaves behind
 * <em>inside a single pod</em>: with the lock alone, every concurrent same-pod request mints its own
 * lock owner, so all of them contend for the same Redis lock and serialize through it one by one
 * (each doing a pub/sub wake-up and a re-check round-trip), even though they all want the very same
 * value. That is exactly the intra-JVM stampede the local "cache the promise, not the value" pattern
 * solves.
 *
 * <p><b>Why a keyed cache and not just {@code .cache()}.</b> Reactor's {@code .cache()} only shares
 * <em>subscribers of one {@code Mono} instance</em>; it does not deduplicate <em>independent</em>
 * {@code getAvailability} calls, each of which builds a brand-new {@code Mono}. To make a burst of
 * same-pod requests share a single in-flight {@code Mono}, something has to hand them all the same
 * instance keyed by {@code eventId}. That store can be a plain {@code ConcurrentHashMap}
 * ({@code computeIfAbsent} also runs its mapping function at most once per key) — no cache library is
 * strictly required. This service uses <b>Caffeine</b> for it because it adds a bounded safety-net
 * eviction ({@code expireAfterWrite} / {@code maximumSize}) for the edge cases a plain map cannot
 * bound — a {@code Mono} that never terminates, or an unbounded key space — and it keeps the local
 * "cache the promise" layer symmetric with the single-JVM module.
 *
 * <pre>{@code
 *  50 same-pod requests        local promise cache           distributed lock (Redis)
 *   |--- get(eventId) -----------> [MISS] builds Mono -----------> tryLock + fetch/recheck
 *   |--- get(eventId) -----------> [HIT]  same in-flight Mono      (only ONE request gets here)
 *   |        ...           -----> [HIT]  same in-flight Mono
 *   |--- get(eventId) -----------> [HIT]  same in-flight Mono
 *   |                              value replayed to all 50        unlock
 * }</pre>
 *
 * <p><b>The local entry must be ephemeral.</b> If the resolved value were kept locally it would shadow
 * Redis: the pod would serve a stale value past the Redis TTL and miss writes from other pods. So the
 * entry only lives while the request is in flight — it is evicted the instant the shared {@code Mono}
 * terminates ({@code doFinally(invalidate)}). Redis stays the single durable, shared cache; this layer
 * is pure in-flight request coalescing. The {@code expireAfterWrite} below is only a safety net in
 * case a {@code Mono} never terminates.
 */
@Service
public class TicketLayeredCacheService {

    private static final Logger log = LoggerFactory.getLogger(TicketLayeredCacheService.class);

    /** Safety net only: entries are normally evicted the instant the in-flight {@code Mono} terminates. */
    private static final Duration IN_FLIGHT_SAFETY_TTL = Duration.ofSeconds(5);

    private final TicketLockCacheService distributedLock;
    private final Cache<String, Mono<TicketAvailability>> inFlight;

    public TicketLayeredCacheService(TicketLockCacheService distributedLock) {
        this.distributedLock = distributedLock;
        this.inFlight = Caffeine.newBuilder()
                .expireAfterWrite(IN_FLIGHT_SAFETY_TTL)
                .build();
    }

    public Mono<TicketAvailability> getAvailability(String eventId) {
        // Caffeine runs the mapping function at most once per key under concurrency, so a burst of
        // same-pod requests shares one in-flight Mono and only one of them enters the distributed lock.
        return inFlight.get(eventId, this::coalesce);
    }

    private Mono<TicketAvailability> coalesce(String eventId) {
        log.debug("local in-flight MISS -> one representative request enters the distributed lock for eventId={}",
                eventId);
        return distributedLock.getAvailability(eventId)
                // Evict as soon as the shared request terminates: Redis remains the durable cache and
                // the local entry only deduplicates the in-flight burst (no local staleness).
                .doFinally(signal -> inFlight.invalidate(eventId))
                // Publish the single in-flight Mono so every concurrent same-pod caller replays it.
                .cache();
    }
}
