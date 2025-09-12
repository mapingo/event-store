package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;

public interface CatchupEventProcessor {

    int processWithEventBuffer(final LinkedEvent event, final String subscriptionName);
}
