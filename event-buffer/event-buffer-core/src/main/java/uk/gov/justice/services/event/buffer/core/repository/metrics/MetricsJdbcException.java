package uk.gov.justice.services.event.buffer.core.repository.metrics;

public class MetricsJdbcException extends RuntimeException {

    public MetricsJdbcException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
