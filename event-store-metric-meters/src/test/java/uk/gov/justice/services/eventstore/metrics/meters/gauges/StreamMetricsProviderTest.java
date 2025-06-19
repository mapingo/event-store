package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;

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

        final String source = "some-source";
        final String component = "some-component";

        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                source,
                component, 234, 329, 23, 87, 78);

        when(logger.isDebugEnabled()).thenReturn(true);
        when(streamMetricsRepository.getStreamMetrics(source, component)).thenReturn(of(eventListenerStreamMetrics));

        assertThat(streamMetricsProvider.getMetrics(source, component), is(of(eventListenerStreamMetrics)));

        verify(logger).debug("Retrieved stream metrics for some-source some-component: Optional[StreamMetrics[source=some-source, component=some-component, streamCount=234, upToDateStreamCount=329, outOfDateStreamCount=23, blockedStreamCount=87, unblockedStreamCount=78]]");
    }

    @Test
    public void shouldNotLogIfLogNotOnDebug() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        final StreamMetrics eventListenerStreamMetrics = new StreamMetrics(
                source,
                component, 234, 329, 23, 87, 78);

        when(logger.isDebugEnabled()).thenReturn(false);
        when(streamMetricsRepository.getStreamMetrics(source, component)).thenReturn(of(eventListenerStreamMetrics));

        assertThat(streamMetricsProvider.getMetrics(source, component), is(of(eventListenerStreamMetrics)));

        verify(logger, never()).debug(anyString());
    }
}