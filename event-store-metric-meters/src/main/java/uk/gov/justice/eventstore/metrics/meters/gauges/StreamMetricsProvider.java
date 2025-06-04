package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;

import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

public class StreamMetricsProvider {

    @Inject
    private StreamMetricsRepository streamMetricsRepository;

    public Optional<StreamMetrics> getMetrics(final String component) {

        final Map<String, StreamMetrics> map = streamMetricsRepository.getStreamMetrics().stream()
                .collect(toMap(StreamMetrics::component, streamMetrics -> streamMetrics));

        return ofNullable(map.get(component));
    }
}
