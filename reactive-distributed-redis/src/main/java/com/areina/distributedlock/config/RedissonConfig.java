package com.areina.distributedlock.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires Redisson as the single Redis client for this module — used for BOTH the value cache
 * ({@code RBucketReactive}) and the distributed lock ({@code RLockReactive}).
 *
 * <p>Spring Data Redis is deliberately absent from the classpath: its native support offers no
 * <em>reactive</em> distributed lock (the only built-in lock, {@code RedisLockRegistry}, is blocking),
 * which would break this project's no-{@code .block()} rule. Redisson is the one client that provides
 * a non-blocking distributed lock, so it owns the value bucket too and the stack stays cohesive.
 *
 * <p>The value is stored as a plain JSON string (see {@link TicketJsonCodec}) over Redisson's
 * {@code StringCodec}, so the bytes in Redis stay human-readable for {@code redis-cli} inspection
 * during the demo.
 */
@Configuration
public class RedissonConfig {

    /**
     * The whole flow is non-blocking, but Redisson connects to Redis eagerly when the client is
     * created — so unlike the Phase 1 module (which boots without PostgreSQL), this app requires a
     * running Redis to start. Bring it up with {@code docker-compose up -d}.
     */
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(
            @Value("${redis.host:localhost}") String host,
            @Value("${redis.port:6379}") int port) {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://%s:%d".formatted(host, port));
        return Redisson.create(config);
    }

    @Bean
    public RedissonReactiveClient redissonReactiveClient(RedissonClient redissonClient) {
        return redissonClient.reactive();
    }
}
