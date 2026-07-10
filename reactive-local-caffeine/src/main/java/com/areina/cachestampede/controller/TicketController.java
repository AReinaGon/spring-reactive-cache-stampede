package com.areina.cachestampede.controller;

import com.areina.cachestampede.model.TicketAvailability;
import com.areina.cachestampede.service.TicketPromiseCacheService;
import com.areina.cachestampede.service.TicketValueCacheService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    private final TicketValueCacheService valueCacheService;
    private final TicketPromiseCacheService promiseCacheService;

    public TicketController(
            TicketValueCacheService valueCacheService,
            TicketPromiseCacheService promiseCacheService) {
        this.valueCacheService = valueCacheService;
        this.promiseCacheService = promiseCacheService;
    }

    /** Mitigated endpoint: the reactive promise cache collapses a burst into a single DB query. */
    @GetMapping("/availability/{eventId}")
    public Mono<TicketAvailability> getAvailability(@PathVariable String eventId) {
        return promiseCacheService.getAvailability(eventId);
    }

    /** Naive endpoint: a value cache that stampedes the DB when a cold key is hit concurrently. */
    @GetMapping("/availability/{eventId}/naive")
    public Mono<TicketAvailability> getAvailabilityNaive(@PathVariable String eventId) {
        return valueCacheService.getAvailability(eventId);
    }
}
