package com.areina.cachestampede.service;

import com.areina.cachestampede.model.TicketAvailability;
import com.areina.cachestampede.repository.TicketRepository;
import com.github.benmanes.caffeine.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Stampede-safe cache implementing the "cache the promise, not the value" pattern.
 *
 * <p>Caffeine stores the in-flight {@code Mono<TicketAvailability>} itself, not its resolved value.
 * Two things make a stampede impossible:
 *
 * <ul>
 *   <li>{@link Cache#get(Object, java.util.function.Function)} runs its mapping function at most once
 *       per key even under concurrent access, so a single {@code Mono} is shared by every caller.
 *   <li>Reactor's {@code .cache()} turns that shared {@code Mono} hot: the source repository is
 *       subscribed only on the first request and the result is replayed to everyone else.
 * </ul>
 *
 * <p>No blocking primitives are used ({@code .block()}, {@code synchronized}, {@code ReentrantLock}
 * are all avoided); the per-key synchronization is provided by Caffeine and the pipeline stays
 * non-blocking end to end.
 */
@Service
public class TicketPromiseCacheService {

    private static final Logger log = LoggerFactory.getLogger(TicketPromiseCacheService.class);

    private final TicketRepository repository;
    private final Cache<String, Mono<TicketAvailability>> promiseCache;

    public TicketPromiseCacheService(
            TicketRepository repository,
            @Qualifier("promiseCache") Cache<String, Mono<TicketAvailability>> promiseCache) {
        this.repository = repository;
        this.promiseCache = promiseCache;
    }

    public Mono<TicketAvailability> getAvailability(String eventId) {
        return promiseCache.get(eventId, this::loadAndCache);
    }

    private Mono<TicketAvailability> loadAndCache(String eventId) {
        log.debug("Cache MISS -> building cached Mono for eventId={}", eventId);
        return repository.findAvailabilityByEventId(eventId).cache();
    }
}
