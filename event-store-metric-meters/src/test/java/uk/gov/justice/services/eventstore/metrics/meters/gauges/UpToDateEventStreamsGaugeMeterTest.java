package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.FRESH_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UpToDateEventStreamsGaugeMeterTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfUpToDateEventStreams() throws Exception {

        final Integer numberOfUpToDateStreams = 266;
        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        final String source = "some-source";
        final String component = "some-component";

        final UpToDateEventStreamsGaugeMeter upToDateEventStreamsGaugeMeter = new UpToDateEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );

        when(streamMetricsProvider.getMetrics(source, component)).thenReturn(of(streamMetrics));
        when(streamMetrics.upToDateStreamCount()).thenReturn(numberOfUpToDateStreams);

        assertThat(upToDateEventStreamsGaugeMeter.measure(), is(numberOfUpToDateStreams));
    }

    @Test
    public void shouldReturnZeroIfNoMetricsFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        final UpToDateEventStreamsGaugeMeter upToDateEventStreamsGaugeMeter = new UpToDateEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );

        when(streamMetricsProvider.getMetrics(source, component)).thenReturn(empty());

        assertThat(upToDateEventStreamsGaugeMeter.measure(), is(0));
    }

    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final UpToDateEventStreamsGaugeMeter upToDateEventStreamsGaugeMeter = new UpToDateEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );

        assertThat(upToDateEventStreamsGaugeMeter.metricName(), is(FRESH_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final UpToDateEventStreamsGaugeMeter upToDateEventStreamsGaugeMeter = new UpToDateEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );

        assertThat(upToDateEventStreamsGaugeMeter.metricDescription(), is("The current number of streams that are up to date"));
    }
}