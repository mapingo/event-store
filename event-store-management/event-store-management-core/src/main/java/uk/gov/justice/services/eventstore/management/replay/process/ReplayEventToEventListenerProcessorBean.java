package uk.gov.justice.services.eventstore.management.replay.process;

import static javax.ejb.TransactionManagementType.CONTAINER;
import static javax.transaction.Transactional.TxType.NEVER;

import uk.gov.justice.services.event.sourcing.subscription.manager.LinkedEventSourceProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.inject.Inject;
import javax.transaction.Transactional;

@Stateless
@TransactionManagement(CONTAINER)
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ReplayEventToEventListenerProcessorBean {

    @Inject
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Inject
    private TransactionReplayEventProcessor transactionReplayEventProcessor;

    @Inject
    private EventConverter eventConverter;


    @Transactional(NEVER)
    public void perform(final ReplayEventContext replayEventContext) {
        final UUID eventId = replayEventContext.getCommandRuntimeId();
        final String source = replayEventContext.getEventSourceName();
        final String component = replayEventContext.getComponentName();

        final LinkedEvent linkedEvent = fetchLinkedEvent(source, eventId);
        process(source, component, linkedEvent);
    }

    private LinkedEvent fetchLinkedEvent(final String eventSourceName, final UUID eventId) {
        final LinkedEventSource linkedEventSource = linkedEventSourceProvider.getLinkedEventSource(eventSourceName);

        return linkedEventSource.findByEventId(eventId)
                .orElseThrow(() -> new ReplayEventFailedException("Event not found for eventId: " + eventId + " from event source name: " + eventSourceName));
    }

    private void process(final String source, final String component, final LinkedEvent linkedEvent) {
        final JsonEnvelope eventEnvelope = eventConverter.envelopeOf(linkedEvent);
        transactionReplayEventProcessor.process(source, component, eventEnvelope);
    }
}
