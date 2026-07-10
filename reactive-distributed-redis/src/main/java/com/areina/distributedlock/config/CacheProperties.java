package com.areina.distributedlock.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Externalised cache tuning, bound from the {@code cache.*} block of {@code application.yml} (and
 * overridable per environment via {@code CACHE_*} variables — see the deployment manifests).
 *
 * <p>The point of pulling these out of the services is the load tests: a 10-minute production TTL is
 * useless when you want to watch a key go cold again and again under JMeter without waiting. Set
 * {@code CACHE_VALUE_TTL=2s} (or call the cache-reset endpoint) and the stampede window reopens on a
 * schedule you control.
 *
 * @param valueTtl         how long a resolved value stays in Redis before it goes cold again
 * @param lockWait         how long a losing pod waits to acquire the distributed lock before failing
 * @param lockLease        lease held by the winning pod (auto-released if it dies mid-fetch)
 * @param localInFlightTtl safety-net eviction for the per-pod in-flight {@code Mono}; entries are
 *                         normally evicted the instant the shared {@code Mono} terminates, so this
 *                         only matters if a {@code Mono} never completes
 */
@ConfigurationProperties(prefix = "cache")
public record CacheProperties(
        @DefaultValue("10m") Duration valueTtl,
        @DefaultValue("30s") Duration lockWait,
        @DefaultValue("10s") Duration lockLease,
        @DefaultValue("5s") Duration localInFlightTtl) {
}
