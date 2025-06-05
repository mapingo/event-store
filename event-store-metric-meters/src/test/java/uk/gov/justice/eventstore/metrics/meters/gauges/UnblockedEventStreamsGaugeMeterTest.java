package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UNBLOCKED_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class UnblockedEventStreamsGaugeMeterTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @Mock
    private Logger logger;

    @InjectMocks
    private UnblockedEventStreamsGaugeMeter unblockedEventStreamsGaugeMeter;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfUnblockedEventStreams() throws Exception {

        final Integer numberOfUnblockedStreams = 266;

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(of(streamMetrics));
        when(logger.isDebugEnabled()).thenReturn(false);
        when(streamMetrics.unblockedStreamCount()).thenReturn(numberOfUnblockedStreams);

        assertThat(unblockedEventStreamsGaugeMeter.measure(), is(numberOfUnblockedStreams));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldReturnZeroIfNoMetricsFound() throws Exception {

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(empty());
        when(logger.isDebugEnabled()).thenReturn(false);

        assertThat(unblockedEventStreamsGaugeMeter.measure(), is(0));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldLogNumberOfUnblockedStreamsIfDebugIsEnabled() throws Exception {

        final Integer numberOfUnblockedStreams = 266;

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(of(streamMetrics));
        when(logger.isDebugEnabled()).thenReturn(true);
        when(streamMetrics.unblockedStreamCount()).thenReturn(numberOfUnblockedStreams);

        assertThat(unblockedEventStreamsGaugeMeter.measure(), is(numberOfUnblockedStreams));
        verify(logger).debug("Micrometer counting number of unblocked EVENT_LISTENER event streams.");
        verify(logger).debug("Number of unblocked EVENT_LISTENER event streams: 266");
    }

    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {
        assertThat(unblockedEventStreamsGaugeMeter.metricName(), is(UNBLOCKED_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {
        assertThat(unblockedEventStreamsGaugeMeter.metricDescription(), is("The current number of streams not in error"));
    }
}