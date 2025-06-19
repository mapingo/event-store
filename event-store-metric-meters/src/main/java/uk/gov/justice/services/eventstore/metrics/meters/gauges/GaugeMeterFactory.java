package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import javax.inject.Inject;

public class GaugeMeterFactory {

    @Inject
    private StreamMetricsProvider streamMetricsProvider;

    public BlockedEventStreamsGaugeMeter createBlockedStreamsGaugeMeter(
            final String source,
            final String component) {

        return new BlockedEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
    }

    public CountEventStreamsGaugeMeter createCountEventStreamsGaugeMeter(
            final String source,
            final String component) {

        return new CountEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
    }

    public OutOfDateEventStreamsGaugeMeter createOutOfDateEventStreamsGaugeMeter(
            final String source,
            final String component) {

        return new OutOfDateEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
    }

    public UnblockedEventStreamsGaugeMeter createUnblockedEventStreamsGaugeMeter(
            final String source,
            final String component) {

        return new UnblockedEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
    }

    public UpToDateEventStreamsGaugeMeter createUpToDateEventStreamsGaugeMeter(
            final String source,
            final String component) {

        return new UpToDateEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
    }
}
