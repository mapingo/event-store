package uk.gov.justice.services.eventsourcing.publishedevent.rebuild;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;

import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

public class RebuildPublishedEventFactory {

    @Inject
    private EventNumberGetter eventNumberGetter;

    @Inject
    private PublishedEventConverter publishedEventConverter;

    public LinkedEvent createPublishedEventFrom(final Event event, final AtomicLong previousEventNumber) {
        final Long eventNumber = eventNumberGetter.eventNumberFrom(event);

        final LinkedEvent linkedEvent = publishedEventConverter.toPublishedEvent(
                event,
                previousEventNumber.get());

        previousEventNumber.set(eventNumber);

        return linkedEvent;
    }
}
