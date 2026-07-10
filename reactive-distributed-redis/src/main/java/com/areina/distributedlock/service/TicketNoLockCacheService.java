package com.areina.distributedlock.service;

import com.areina.distributedlock.config.CacheProperties;
import com.areina.distributedlock.config.TicketJsonCodec;
import com.areina.distributedlock.model.TicketAvailability;
import com.areina.distributedlock.repository.TicketRepository;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Naive distributed cache: stores the resolved value in Redis with no cross-pod coordination — the
 * distributed cache stampede.
 *
 * <p>This is the Phase 2 analogue of Phase 1's naive value cache, but the miss now spans the whole
 * cluster. When a popular key is cold or has just expired, every pod reads Redis, every pod misses at
 * the same instant, and every pod queries the database before any of them has written the value back.
 * One expired key turns into one redundant database query <em>per pod</em>.
 */
@Service
public class TicketNoLockCacheService {

    private static final Logger log = LoggerFactory.getLogger(TicketNoLockCacheService.class);
    private static final String KEY_PREFIX = "tickets:availability:";

    private final TicketRepository repository;
    private final RedissonReactiveClient redisson;
    private final TicketJsonCodec codec;
    private final CacheProperties cacheProperties;
    private final String podId;

    public TicketNoLockCacheService(
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
                .switchIfEmpty(Mono.defer(() -> fetchAndCache(eventId, bucket)));
    }

    private Mono<TicketAvailability> fetchAndCache(String eventId, RBucketReactive<String> bucket) {
        log.debug("[{}] Redis MISS -> querying DB (no coordination) eventId={}", podId, eventId);
        return repository.findAvailabilityByEventId(eventId)
                .map(value -> value.handledBy(podId))
                .flatMap(value -> bucket.set(codec.encode(value), cacheProperties.valueTtl()).thenReturn(value));
    }
}
