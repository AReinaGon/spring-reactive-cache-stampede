package com.areina.distributedlock.model;

import java.time.LocalDateTime;

/**
 * Snapshot of ticket availability for an event, stored in Redis as JSON.
 *
 * <p>{@code queriedAt} records the exact moment the database was hit, and {@code handledByPod}
 * records which simulated instance ("pod") actually ran that query. Together they are the signal the
 * distributed stampede test relies on: if every pod in the cluster observes the same {@code queriedAt}
 * and the same {@code handledByPod}, the database was queried exactly once, by a single pod.
 */
public record TicketAvailability(
        String eventId,
        String eventName,
        int availableSeats,
        LocalDateTime queriedAt,
        String handledByPod) {

    /** Returns a copy stamped with the id of the pod that produced this value. */
    public TicketAvailability handledBy(String podId) {
        return new TicketAvailability(eventId, eventName, availableSeats, queriedAt, podId);
    }
}
