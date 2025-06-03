package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.lang.String.format;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.OUT_OF_DATE_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UP_TO_DATE_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import javax.inject.Inject;

import org.slf4j.Logger;

public class UpToDateEventStreamsGaugeMeter implements GaugeMetricsMeter {

    @Inject
    private Logger logger;

    @Inject
    private EventMetricsRepository eventMetricsRepository;

    @Override
    public int measure() {
        final Integer numberOfUpToDateStreams = eventMetricsRepository.countUpToDateStreams();

        if (logger.isDebugEnabled()) {
            logger.debug(format("Micrometer counting number of up to date event streams. Number of up-to-date streams: %d", numberOfUpToDateStreams));
        }

        return numberOfUpToDateStreams;
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
