package uk.gov.justice.services.resources.application.version.rest.model;

public class ProjectVersionException extends RuntimeException {

    public ProjectVersionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
