package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.lang.String.format;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.COUNT_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import javax.inject.Inject;

import org.slf4j.Logger;

public class BlockedEventStreamsGaugeMeter implements GaugeMetricsMeter {

    @Inject
    private Logger logger;

    @Inject
    private EventMetricsRepository eventMetricsRepository;

    @Override
    public int measure() {
        final Integer numberOfBlockedStreams = eventMetricsRepository.countBlockedStreams();

        if (logger.isDebugEnabled()) {
            logger.debug(format("Micrometer counting number of blocked event streams. Number of blocked streams: %d", numberOfBlockedStreams));
        }

        return numberOfBlockedStreams;
    }

    @Override
    public String metricName() {
         return BLOCKED_EVENT_STREAMS_GAUGE_NAME;
    }

    @Override
    public String metricDescription() {
        return "The current number of streams that are blocked due to errors";
    }
}
