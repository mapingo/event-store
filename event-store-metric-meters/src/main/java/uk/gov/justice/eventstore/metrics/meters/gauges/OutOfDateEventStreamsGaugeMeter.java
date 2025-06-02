package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.lang.String.format;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.OUT_OF_DATE_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import javax.inject.Inject;

import org.slf4j.Logger;

public class OutOfDateEventStreamsGaugeMeter implements GaugeMetricsMeter {

    @Inject
    private Logger logger;

    @Inject
    private EventMetricsRepository eventMetricsRepository;

    @Override
    public int measure() {
        final Integer numberOfBlockedStreams = eventMetricsRepository.countOutOfDateStreams();

        if (logger.isDebugEnabled()) {
            logger.debug(format("Micrometer counting number of out of date event streams. Number of out-of-date streams: %d", numberOfBlockedStreams));
        }

        return numberOfBlockedStreams;
    }

    @Override
    public String metricName() {
         return OUT_OF_DATE_EVENT_STREAMS_GAUGE_NAME;
    }

    @Override
    public String metricDescription() {
        return "The current number of streams that are out of date";
    }
}
