package com.areina.cachestampede.config;

import com.areina.cachestampede.model.TicketAvailability;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Two Caffeine caches that embody the difference between a naive and a stampede-safe design.
 *
 * <p>Both share the same TTL ({@code cache.ttl}, default 10s — short, to keep the load test fast;
 * production would use minutes). A shorter TTL makes the key expire more often, so under sustained
 * load the naive value cache re-stampedes on every expiry. What differs is <em>what</em> they store:
 *
 * <ul>
 *   <li>{@code valueCache} stores the resolved {@link TicketAvailability}. It only holds an entry
 *       <em>after</em> the database has answered, so a concurrent burst on a cold or just-expired
 *       key all miss together and stampede the database.
 *   <li>{@code promiseCache} stores the in-flight {@code Mono} itself. The promise is published the
 *       instant the first caller arrives, so the whole burst shares it and the database is queried
 *       once.
 * </ul>
 *
 * <p>Spring's {@code @Cacheable} cannot cache a {@code Mono<T>} (it would resolve and store the
 * value, collapsing it back into the naive case), so the caches are built and managed by hand.
 */
@Configuration
public class CacheConfig {

    private final Duration ttl;

    public CacheConfig(@Value("${cache.ttl:10s}") Duration ttl) {
        this.ttl = ttl;
    }

    /** Naive cache: stores the resolved value, populated only after the DB answers. */
    @Bean
    public Cache<String, TicketAvailability> valueCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(10_000)
                .recordStats()
                .build();
    }

    /** Stampede-safe cache: stores the in-flight reactive promise. */
    @Bean
    public Cache<String, Mono<TicketAvailability>> promiseCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(10_000)
                .recordStats()
                .build();
    }
}
