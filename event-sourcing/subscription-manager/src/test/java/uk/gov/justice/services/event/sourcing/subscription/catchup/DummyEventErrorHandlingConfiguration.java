package uk.gov.justice.services.event.sourcing.subscription.catchup;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;

public class DummyEventErrorHandlingConfiguration implements EventErrorHandlingConfiguration {

    @Override
    public boolean isEventStreamSelfHealingEnabled() {
        return true;
    }
}
