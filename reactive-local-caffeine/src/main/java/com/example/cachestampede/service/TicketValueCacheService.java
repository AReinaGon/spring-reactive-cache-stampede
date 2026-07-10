package com.example.cachestampede.service;

import com.example.cachestampede.model.TicketAvailability;
import com.example.cachestampede.repository.TicketRepository;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Naive cache that stores the resolved VALUE — the classic cache stampede.
 *
 * <p>This service DOES use a cache, but it stores the resolved {@link TicketAvailability} value.
 * The entry only exists once the database has answered, so on a cold or just-expired key every
 * concurrent caller sees a miss at the same instant, and they all query the database before any of
 * them gets a chance to populate the cache. That synchronized rush of redundant queries is the
 * stampede this PoC sets out to demonstrate.
 *
 * <p>The cache lookup lives inside {@link Mono#defer} so the miss is evaluated per subscription,
 * faithfully reproducing what happens at the moment a TTL expires under load.
 */
@Service
public class TicketValueCacheService {

    private final TicketRepository repository;
    private final Cache<String, TicketAvailability> valueCache;

    public TicketValueCacheService(
            TicketRepository repository,
            @Qualifier("valueCache") Cache<String, TicketAvailability> valueCache) {
        this.repository = repository;
        this.valueCache = valueCache;
    }

    public Mono<TicketAvailability> getAvailability(String eventId) {
        return Mono.defer(() -> {
            TicketAvailability cached = valueCache.getIfPresent(eventId);
            if (cached != null) {
                return Mono.just(cached);
            }
            return repository.findAvailabilityByEventId(eventId)
                    .doOnNext(value -> valueCache.put(eventId, value));
        });
    }
}
