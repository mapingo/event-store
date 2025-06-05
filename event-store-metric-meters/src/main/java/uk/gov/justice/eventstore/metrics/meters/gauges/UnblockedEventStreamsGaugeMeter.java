package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.lang.String.format;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UNBLOCKED_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import javax.inject.Inject;

import org.slf4j.Logger;

public class UnblockedEventStreamsGaugeMeter implements GaugeMetricsMeter {

    @Inject
    private Logger logger;

    @Inject
    private StreamMetricsProvider streamMetricsProvider;

    @Override
    public int measure() {

        final String component = EVENT_LISTENER;
        if (logger.isDebugEnabled()) {
            logger.debug(format("Micrometer counting number of unblocked %s event streams.", component));
        }

        final int blockedStreamCount = streamMetricsProvider
                .getMetrics(component)
                .map(StreamMetrics::unblockedStreamCount)
                .orElse(0);

        if (logger.isDebugEnabled()) {
            logger.debug(format("Number of unblocked %s event streams: %d", component, blockedStreamCount));
        }

        return blockedStreamCount;
    }

    @Override
    public String metricName() {
        return UNBLOCKED_EVENT_STREAMS_GAUGE_NAME;
    }

    @Override
    public String metricDescription() {
        return "The current number of streams not in error";
    }
}
