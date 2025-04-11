package uk.gov.justice.services.event.sourcing.subscription.manager;

import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_ALREADY_PROCESSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_OUT_OF_ORDER;

import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;

public class EventProcessingStatusCalculator {

    public EventOrderingStatus calculateEventOrderingStatus(final StreamPositions streamPositions) {

        final long incomingEventPosition = streamPositions.incomingEventPosition();
        final long currentStreamPosition = streamPositions.currentStreamPosition();

        if(incomingEventPosition <= currentStreamPosition) {
            return EVENT_ALREADY_PROCESSED;
        }

        if (incomingEventPosition - currentStreamPosition != 1) {
            return EVENT_OUT_OF_ORDER;
        }

        return EVENT_CORRECTLY_ORDERED;
    }
}
