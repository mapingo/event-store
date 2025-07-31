package uk.gov.justice.services.event.buffer.core.repository.subscription;

public class StreamStatusLockingException extends RuntimeException{

    public StreamStatusLockingException(final String message) {
        super(message);
    }

    public StreamStatusLockingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
