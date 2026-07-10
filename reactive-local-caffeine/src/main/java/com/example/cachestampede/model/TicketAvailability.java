package com.example.cachestampede.model;

import java.time.LocalDateTime;

/**
 * Snapshot of ticket availability for an event.
 *
 * <p>{@code queriedAt} records the exact moment the (simulated) database was hit. It is the key
 * signal the stampede test relies on: if every concurrent caller observes the same timestamp, the
 * database was queried exactly once.
 */
public record TicketAvailability(
        String eventId,
        String eventName,
        int availableSeats,
        LocalDateTime queriedAt) {
}
