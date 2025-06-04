package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.lang.String.format;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UP_TO_DATE_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import javax.inject.Inject;

import org.slf4j.Logger;

public class UpToDateEventStreamsGaugeMeter implements GaugeMetricsMeter {

    @Inject
    private Logger logger;

    @Inject
    private StreamMetricsProvider streamMetricsProvider;

    @Override
    public int measure() {
        final String component = EVENT_LISTENER;
        if (logger.isDebugEnabled()) {
            logger.debug(format("Micrometer counting number of up to date %s event streams.", component));
        }

        final int eventStreamCount = streamMetricsProvider
                .getMetrics(component)
                .map(StreamMetrics::upToDateStreamCount)
                .orElse(0);

        if (logger.isDebugEnabled()) {
            logger.debug(format("Number of up to date %s event streams: %d", component, eventStreamCount));
        }

        return eventStreamCount;
    }

    @Override
    public String metricName() {
         return UP_TO_DATE_EVENT_STREAMS_GAUGE_NAME;
    }

    @Override
    public String metricDescription() {
        return "The current number of streams that are up to date";
    }
}
