package com.areina.distributedlock.config;

import com.areina.distributedlock.model.TicketAvailability;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Converts {@link TicketAvailability} to/from the JSON string stored in Redis.
 *
 * <p>Uses Spring Boot's autoconfigured Jackson 3 {@link ObjectMapper}, which already has record and
 * {@code java.time} support set up. This sidesteps Redisson's bundled codecs: Kryo5 cannot
 * instantiate records (no no-arg constructor) and its JSON codec targets Jackson 2, which is not on
 * this Spring Boot 4 / Jackson 3 classpath.
 */
@Component
public class TicketJsonCodec {

    private final ObjectMapper objectMapper;

    public TicketJsonCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(TicketAvailability value) {
        return objectMapper.writeValueAsString(value);
    }

    public TicketAvailability decode(String json) {
        return objectMapper.readValue(json, TicketAvailability.class);
    }
}
