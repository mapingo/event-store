package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BlockedEventStreamsGaugeMeterTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;
    
    @Test
    public void shouldGetTheCountOfTheTheNumberOfBlockedEventStreams() throws Exception {

        final Integer numberOfBlockedStreams = 266;
        final String source = "some-source";
        final String component = "some-component";

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        final BlockedEventStreamsGaugeMeter blockedEventStreamsGaugeMeter = new BlockedEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider);

        when(streamMetricsProvider.getMetrics(source, component)).thenReturn(of(streamMetrics));
        when(streamMetrics.blockedStreamCount()).thenReturn(numberOfBlockedStreams);

        assertThat(blockedEventStreamsGaugeMeter.measure(), is(numberOfBlockedStreams));
    }

    @Test
    public void shouldReturnZeroIfNoMetricsFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final BlockedEventStreamsGaugeMeter blockedEventStreamsGaugeMeter = new BlockedEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider);

        when(streamMetricsProvider.getMetrics(source, component)).thenReturn(empty());

        assertThat(blockedEventStreamsGaugeMeter.measure(), is(0));
    }


    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        final BlockedEventStreamsGaugeMeter blockedEventStreamsGaugeMeter = new BlockedEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider);

        assertThat(blockedEventStreamsGaugeMeter.metricName(), is(BLOCKED_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        final BlockedEventStreamsGaugeMeter blockedEventStreamsGaugeMeter = new BlockedEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider);

        assertThat(blockedEventStreamsGaugeMeter.metricDescription(), is("The current number of streams that are blocked due to errors"));
    }
}