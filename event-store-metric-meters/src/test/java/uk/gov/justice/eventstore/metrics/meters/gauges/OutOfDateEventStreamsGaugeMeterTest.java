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
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.OUT_OF_DATE_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class OutOfDateEventStreamsGaugeMeterTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @Mock
    private Logger logger;

    @InjectMocks
    private OutOfDateEventStreamsGaugeMeter outOfDateEventStreamsGaugeMeter;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfOutOfDateEventStreams() throws Exception {

        final Integer numberOfOutOfDateStreams = 266;
        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(of(streamMetrics));
        when(logger.isDebugEnabled()).thenReturn(true);
        when(streamMetrics.outOfDateStreamCount()).thenReturn(numberOfOutOfDateStreams);

        assertThat(outOfDateEventStreamsGaugeMeter.measure(), is(numberOfOutOfDateStreams));
        verify(logger).debug("Micrometer counting number of out of date EVENT_LISTENER event streams.");
        verify(logger).debug("Number of out of date EVENT_LISTENER event streams: 266");
    }

    @Test
    public void shouldReturnZeroIfNoMetricsFound() throws Exception {

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(empty());
        when(logger.isDebugEnabled()).thenReturn(false);

        assertThat(outOfDateEventStreamsGaugeMeter.measure(), is(0));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldLogNumberOfOutOfDateStreamsIfDebugIsEnabled() throws Exception {

        final Integer numberOfOutOfDateStreams = 266;

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(of(streamMetrics));
        when(logger.isDebugEnabled()).thenReturn(true);
        when(streamMetrics.outOfDateStreamCount()).thenReturn(numberOfOutOfDateStreams);

        assertThat(outOfDateEventStreamsGaugeMeter.measure(), is(numberOfOutOfDateStreams));
        verify(logger).debug("Micrometer counting number of out of date EVENT_LISTENER event streams.");
        verify(logger).debug("Number of out of date EVENT_LISTENER event streams: 266");
    }

    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {

        assertThat(outOfDateEventStreamsGaugeMeter.metricName(), is(OUT_OF_DATE_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {

        assertThat(outOfDateEventStreamsGaugeMeter.metricDescription(), is("The current number of streams that are out of date"));
    }
}