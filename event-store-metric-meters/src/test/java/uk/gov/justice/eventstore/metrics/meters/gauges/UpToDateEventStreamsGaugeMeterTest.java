package uk.gov.justice.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.OUT_OF_DATE_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UP_TO_DATE_EVENT_STREAMS_GAUGE_NAME;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class UpToDateEventStreamsGaugeMeterTest {

    @Mock
    private EventMetricsRepository eventMetricsRepository;

    @Mock
    private Logger logger;

    @InjectMocks
    private UpToDateEventStreamsGaugeMeter upToDateEventStreamsGaugeMeter;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfUpToDateEventStreams() throws Exception {

        final Integer numberOfUpToDateStreams = 266;

        when(eventMetricsRepository.countUpToDateStreams()).thenReturn(numberOfUpToDateStreams);
        when(logger.isDebugEnabled()).thenReturn(false);

        assertThat(upToDateEventStreamsGaugeMeter.measure(), is(numberOfUpToDateStreams));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldLogNumberOfUpToDateStreamsIfDebugIsEnabled() throws Exception {

        final Integer numberOfUpToDateStreams = 23;

        when(eventMetricsRepository.countUpToDateStreams()).thenReturn(numberOfUpToDateStreams);
        when(logger.isDebugEnabled()).thenReturn(true);

        assertThat(upToDateEventStreamsGaugeMeter.measure(), is(numberOfUpToDateStreams));

        verify(logger).debug("Micrometer counting number of up to date event streams. Number of up-to-date streams: 23");
    }

    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {

        assertThat(upToDateEventStreamsGaugeMeter.metricName(), is(UP_TO_DATE_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {

        assertThat(upToDateEventStreamsGaugeMeter.metricDescription(), is("The current number of streams that are up to date"));
    }
}