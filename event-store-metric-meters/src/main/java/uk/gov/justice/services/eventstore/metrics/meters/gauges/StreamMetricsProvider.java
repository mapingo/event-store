package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.slf4j.Logger;

public class StreamMetricsProvider {

    @Inject
    private StreamMetricsRepository streamMetricsRepository;

    @Inject
    private Logger logger;

    public Optional<StreamMetrics> getMetrics(final String component) {

        final List<StreamMetrics> streamMetricsList = streamMetricsRepository.getStreamMetrics();

        if (logger.isDebugEnabled()) {
            logger.debug(format("Retrieved stream metrics for %s: %s", component, streamMetricsList));
        }

        final Map<String, StreamMetrics> map = streamMetricsList.stream()
                .collect(toMap(StreamMetrics::component, streamMetrics -> streamMetrics));

        return ofNullable(map.get(component));
    }
}
