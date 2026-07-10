package com.areina.distributedlock.controller;

import com.areina.distributedlock.model.TicketAvailability;
import com.areina.distributedlock.service.TicketLayeredCacheService;
import com.areina.distributedlock.service.TicketLockCacheService;
import com.areina.distributedlock.service.TicketNoLockCacheService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    private final TicketLockCacheService lockCacheService;
    private final TicketLayeredCacheService layeredCacheService;
    private final TicketNoLockCacheService noLockCacheService;

    public TicketController(
            TicketLockCacheService lockCacheService,
            TicketLayeredCacheService layeredCacheService,
            TicketNoLockCacheService noLockCacheService) {
        this.lockCacheService = lockCacheService;
        this.layeredCacheService = layeredCacheService;
        this.noLockCacheService = noLockCacheService;
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
}
