package uk.gov.justice.services.event.sourcing.subscription.error;

public class MissingPositionInStreamException extends RuntimeException {

    public MissingPositionInStreamException(final String message) {
        super(message);
    }
}
