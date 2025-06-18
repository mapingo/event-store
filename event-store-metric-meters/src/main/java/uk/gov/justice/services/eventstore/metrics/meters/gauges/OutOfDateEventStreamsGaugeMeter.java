package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.STALE_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

public class OutOfDateEventStreamsGaugeMeter implements GaugeMetricsMeter {

    private final String source;
    private final String component;
    private final StreamMetricsProvider streamMetricsProvider;

    public OutOfDateEventStreamsGaugeMeter(final String source, final String component, final StreamMetricsProvider streamMetricsProvider) {
        this.source = source;
        this.component = component;
        this.streamMetricsProvider = streamMetricsProvider;
    }

    @Override
    public int measure() {

        return streamMetricsProvider
                .getMetrics(source, component)
                .map(StreamMetrics::outOfDateStreamCount)
                .orElse(0);
    }

    @Override
    public String metricName() {
         return STALE_EVENT_STREAMS_GAUGE_NAME;
    }

    @Override
    public String metricDescription() {
        return "The current number of streams that are out of date";
    }
}
