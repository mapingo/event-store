package uk.gov.justice.services.event.sourcing.subscription.manager;

public enum EventOrderingStatus {

    /**
     * An event is correctly ordered if the incoming event's positionInStream
     * is one more than the current position in the stream_status table
     */
    EVENT_CORRECTLY_ORDERED,

    /**
     * An event is out of order if the incoming event's positionInStream
     * is more than 1 greater the current position in the stream_status table
     */
    EVENT_OUT_OF_ORDER,

    /**
     * An event is already processed if the incoming event's positionInStream
     * is less than or equal to the current position in the stream_status table
     */
    EVENT_ALREADY_PROCESSED
}
