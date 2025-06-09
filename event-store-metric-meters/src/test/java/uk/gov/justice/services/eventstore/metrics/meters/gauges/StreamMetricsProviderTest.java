package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;
import uk.gov.justice.services.eventstore.metrics.meters.gauges.StreamMetricsProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamMetricsProviderTest {

    @Mock
    private StreamMetricsRepository streamMetricsRepository;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamMetricsProvider streamMetricsProvider;

    @Test
    public void shouldFindTheCorrectMetricsForComponent() throws Exception {

        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_LISTENER, 234, 329, 23, 87, 78);
        final StreamMetrics eventProcessorStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_PROCESSOR, 23, 64, 25, 477, 34);

        when(logger.isDebugEnabled()).thenReturn(true);
        when(streamMetricsRepository.getStreamMetrics()).thenReturn(asList(eventListenerStreamMetrics, eventProcessorStreamMetrics));

        assertThat(streamMetricsProvider.getMetrics(eventListenerStreamMetrics.component()), is(of(eventListenerStreamMetrics)));

        verify(logger).debug("Retrieved stream metrics for EVENT_LISTENER: [StreamMetrics[source=hearing, component=EVENT_LISTENER, streamCount=234, upToDateStreamCount=329, outOfDateStreamCount=23, blockedStreamCount=87, unblockedStreamCount=78], StreamMetrics[source=hearing, component=EVENT_PROCESSOR, streamCount=23, upToDateStreamCount=64, outOfDateStreamCount=25, blockedStreamCount=477, unblockedStreamCount=34]]");
    }

    @Test
    public void shouldNotLogIfLogNotOnDebug() throws Exception {

        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_LISTENER, 234, 329, 23, 87, 78);
        final StreamMetrics eventProcessorStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_PROCESSOR, 23, 64, 25, 477, 34);

        when(logger.isDebugEnabled()).thenReturn(false);
        when(streamMetricsRepository.getStreamMetrics()).thenReturn(asList(eventListenerStreamMetrics, eventProcessorStreamMetrics));

        assertThat(streamMetricsProvider.getMetrics(eventListenerStreamMetrics.component()), is(of(eventListenerStreamMetrics)));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldReturnEmptyIfNoMetricsFoundForComponent() throws Exception {
        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_LISTENER, 234, 329, 23, 87, 78);
        final StreamMetrics eventProcessorStreamMetrics = new StreamMetrics(
                "hearing",
                EVENT_PROCESSOR, 23, 64, 25, 477, 34);

        when(streamMetricsRepository.getStreamMetrics()).thenReturn(asList(eventListenerStreamMetrics, eventProcessorStreamMetrics));

        assertThat(streamMetricsProvider.getMetrics("some-other-component"), is(empty()));
    }
}