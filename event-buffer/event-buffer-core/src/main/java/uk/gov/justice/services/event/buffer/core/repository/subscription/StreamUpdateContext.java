package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.util.Optional;
import java.util.UUID;

public record StreamUpdateContext(
        long incomingEventPosition,
        long currentStreamPosition,
        long latestKnownStreamPosition,
        Optional<UUID> streamErrorId) {

    public boolean streamCurrentlyErrored() {
        return streamErrorId.isPresent();
    }
}
