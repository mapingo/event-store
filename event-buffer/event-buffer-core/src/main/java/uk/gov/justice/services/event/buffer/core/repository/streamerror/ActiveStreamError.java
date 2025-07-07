package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.util.Optional;

public record ActiveStreamError(
        String hash,
        String exceptionClassname,
        Optional<String> causeClassname,
        String javaClassname,
        String javaMethod,
        int javaLineNumber,
        int affectedStreamsCount,
        int affectedEventsCount) {
}