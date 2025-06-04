package uk.gov.justice.eventstore.metrics.meters.gauges;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_PROCESSOR;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;

import java.util.Optional;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamMetricsProviderTest {

    @Mock
    private StreamMetricsRepository streamMetricsRepository;

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

        when(streamMetricsRepository.getStreamMetrics()).thenReturn(asList(eventListenerStreamMetrics, eventProcessorStreamMetrics));

        assertThat(streamMetricsProvider.getMetrics(eventListenerStreamMetrics.component()), is(of(eventListenerStreamMetrics)));
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