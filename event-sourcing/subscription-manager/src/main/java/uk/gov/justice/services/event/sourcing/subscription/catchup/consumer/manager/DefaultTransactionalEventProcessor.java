package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.event.sourcing.subscription.manager.CatchupEventBufferProcessor;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class DefaultTransactionalEventProcessor {

    @Inject
    private EventConverter eventConverter;

    @Inject
    private CatchupEventBufferProcessor catchupEventBufferProcessor;

    @Transactional(REQUIRES_NEW)
    public int processWithEventBuffer(final LinkedEvent linkedEvent, final String subscriptionName) {
        final JsonEnvelope eventEnvelope = eventConverter.envelopeOf(linkedEvent);
        catchupEventBufferProcessor.processWithEventBuffer(eventEnvelope, subscriptionName);
        return 1;
    }
}
