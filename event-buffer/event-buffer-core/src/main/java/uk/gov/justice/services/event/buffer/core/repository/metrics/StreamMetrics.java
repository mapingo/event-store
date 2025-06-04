package uk.gov.justice.services.event.buffer.core.repository.metrics;

public record StreamMetrics(
        String source,
        String component,
        int streamCount,
        int upToDateStreamCount,
        int outOfDateStreamCount,
        int blockedStreamCount,
        int unblockedStreamCount) {
}
