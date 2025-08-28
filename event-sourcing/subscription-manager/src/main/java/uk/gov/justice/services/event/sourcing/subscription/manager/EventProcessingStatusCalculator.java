package uk.gov.justice.services.event.sourcing.subscription.manager;

import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_ALREADY_PROCESSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_OUT_OF_ORDER;

import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;

public class EventProcessingStatusCalculator {

    public EventOrderingStatus calculateEventOrderingStatus(final StreamUpdateContext streamUpdateContext) {

        final long incomingEventPosition = streamUpdateContext.incomingEventPosition();
        final long currentStreamPosition = streamUpdateContext.currentStreamPosition();

        if(incomingEventPosition <= currentStreamPosition) {
            return EVENT_ALREADY_PROCESSED;
        }

        if (incomingEventPosition - currentStreamPosition != 1) {
            return EVENT_OUT_OF_ORDER;
        }

        return EVENT_CORRECTLY_ORDERED;
    }
}
