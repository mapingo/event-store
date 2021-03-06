package uk.gov.justice.services.eventsourcing.util.messaging;

import static org.apache.commons.lang3.StringUtils.substringBefore;

import uk.gov.justice.services.messaging.JsonEnvelope;

public class EventSourceNameCalculator {

    public String getSource(final JsonEnvelope event) {
        return substringBefore(event.metadata().name(), ".");
    }
}
