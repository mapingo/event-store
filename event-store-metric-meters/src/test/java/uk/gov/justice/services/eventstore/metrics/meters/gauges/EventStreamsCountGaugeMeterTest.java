package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.TOTAL_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventStreamsCountGaugeMeterTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;


    @Test
    public void shouldGetTheCountOfTheTheNumberOfEventStreams() throws Exception {

        final Integer numberOfStreams = 23;
        final String source = "some-source";
        final String component = "some-component";

        final CountEventStreamsGaugeMeter countEventStreamsGaugeMeter = new CountEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );

        final StreamMetrics streamMetrics = mock(StreamMetrics.class);

        when(streamMetricsProvider.getMetrics(source, component)).thenReturn(of(streamMetrics));
        when(streamMetrics.streamCount()).thenReturn(numberOfStreams);

        assertThat(countEventStreamsGaugeMeter.measure(), is(numberOfStreams));
    }

    @Test
    public void shouldReturnZeroIfNoMetricsFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        final CountEventStreamsGaugeMeter countEventStreamsGaugeMeter = new CountEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
        when(streamMetricsProvider.getMetrics(source, component)).thenReturn(empty());

        assertThat(countEventStreamsGaugeMeter.measure(), is(0));
    }
    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final CountEventStreamsGaugeMeter countEventStreamsGaugeMeter = new CountEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
        assertThat(countEventStreamsGaugeMeter.metricName(), is(TOTAL_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final CountEventStreamsGaugeMeter countEventStreamsGaugeMeter = new CountEventStreamsGaugeMeter(
                source,
                component,
                streamMetricsProvider
        );
        assertThat(countEventStreamsGaugeMeter.metricDescription(), is("The total number of event streams"));
    }
}