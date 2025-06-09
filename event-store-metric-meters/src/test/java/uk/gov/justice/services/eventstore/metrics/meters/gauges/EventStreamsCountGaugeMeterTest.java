package uk.gov.justice.services.eventstore.metrics.meters.gauges;

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
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.COUNT_EVENT_STREAMS_GAUGE_NAME;


import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.eventstore.metrics.meters.gauges.CountEventStreamsGaugeMeter;
import uk.gov.justice.services.eventstore.metrics.meters.gauges.StreamMetricsProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventStreamsCountGaugeMeterTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @Mock
    private Logger logger;

    @InjectMocks
    private CountEventStreamsGaugeMeter countEventStreamsGaugeMeter;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfEventStreams() throws Exception {

        final Integer numberOfStreams = 23;

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(of(streamMetrics));
        when(logger.isDebugEnabled()).thenReturn(false);
        when(streamMetrics.streamCount()).thenReturn(numberOfStreams);

        assertThat(countEventStreamsGaugeMeter.measure(), is(numberOfStreams));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldReturnZeroIfNoMetricsFound() throws Exception {

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(empty());
        when(logger.isDebugEnabled()).thenReturn(false);

        assertThat(countEventStreamsGaugeMeter.measure(), is(0));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldLogNumberOfStreamsIfDebugIsEnabled() throws Exception {

        final Integer numberOfStreams = 23;

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(EVENT_LISTENER)).thenReturn(of(streamMetrics));
        when(logger.isDebugEnabled()).thenReturn(true);
        when(streamMetrics.streamCount()).thenReturn(numberOfStreams);

        assertThat(countEventStreamsGaugeMeter.measure(), is(numberOfStreams));
        verify(logger).debug("Micrometer counting number of EVENT_LISTENER event streams.");
        verify(logger).debug("Number of EVENT_LISTENER event streams: 23");
    }

    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {
        assertThat(countEventStreamsGaugeMeter.metricName(), is(COUNT_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {
        assertThat(countEventStreamsGaugeMeter.metricDescription(), is("The total number of event streams"));
    }
}