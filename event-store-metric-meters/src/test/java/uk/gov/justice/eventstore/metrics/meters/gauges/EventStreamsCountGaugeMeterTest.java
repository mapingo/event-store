package uk.gov.justice.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.COUNT_EVENT_STREAMS_GAUGE_NAME;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventStreamsCountGaugeMeterTest {

    @Mock
    private EventMetricsRepository eventMetricsRepository;

    @Mock
    private Logger logger;

    @InjectMocks
    private CountEventStreamsGaugeMeter countEventStreamsGaugeMeter;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfEventStreams() throws Exception {

        final Integer numberOfStreams = 23;

        when(eventMetricsRepository.countStreams()).thenReturn(numberOfStreams);
        when(logger.isDebugEnabled()).thenReturn(false);

        assertThat(countEventStreamsGaugeMeter.measure(), is(numberOfStreams));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldLogNumberOfStreamsIfDebugIsEnabled() throws Exception {

        final Integer numberOfStreams = 23;

        when(eventMetricsRepository.countStreams()).thenReturn(numberOfStreams);
        when(logger.isDebugEnabled()).thenReturn(true);

        assertThat(countEventStreamsGaugeMeter.measure(), is(numberOfStreams));

        verify(logger).debug("Micrometer counting number of event streams. Number of streams: 23");
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