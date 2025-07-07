package uk.gov.justice.services.event.buffer.core.repository.streamerror;

public class StreamErrorPersistenceException extends RuntimeException {

    public StreamErrorPersistenceException(final String message) {
        super(message);
    }

    public StreamErrorPersistenceException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
