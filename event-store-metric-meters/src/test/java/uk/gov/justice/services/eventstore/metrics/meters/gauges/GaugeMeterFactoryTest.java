package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GaugeMeterFactoryTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @InjectMocks
    private GaugeMeterFactory gaugeMeterFactory;

    @Test
    public void shouldCreateNewBlockedEventStreamsGaugeMeter() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final BlockedEventStreamsGaugeMeter blockedStreamsGaugeMeter = gaugeMeterFactory.createBlockedStreamsGaugeMeter(
                source,
                component);

        assertThat(getValueOfField(blockedStreamsGaugeMeter, "source", String.class), is(source));
        assertThat(getValueOfField(blockedStreamsGaugeMeter, "component", String.class), is(component));
        assertThat(getValueOfField(blockedStreamsGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
    }

    @Test
    public void shouldCreateNewCountEventStreamsGaugeMeter() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final CountEventStreamsGaugeMeter countEventStreamsGaugeMeter = gaugeMeterFactory.createCountEventStreamsGaugeMeter(
                source,
                component);

        assertThat(getValueOfField(countEventStreamsGaugeMeter, "source", String.class), is(source));
        assertThat(getValueOfField(countEventStreamsGaugeMeter, "component", String.class), is(component));
        assertThat(getValueOfField(countEventStreamsGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
    }

    @Test
    public void shouldCreateNewOutOfDateEventStreamsGaugeMeter() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final OutOfDateEventStreamsGaugeMeter countEventStreamsGaugeMeter = gaugeMeterFactory.createOutOfDateEventStreamsGaugeMeter(
                source,
                component);

        assertThat(getValueOfField(countEventStreamsGaugeMeter, "source", String.class), is(source));
        assertThat(getValueOfField(countEventStreamsGaugeMeter, "component", String.class), is(component));
        assertThat(getValueOfField(countEventStreamsGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
    }

    @Test
    public void shouldCreateNewUnblockedEventStreamsGaugeMeter() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final UnblockedEventStreamsGaugeMeter unblockedEventStreamsGaugeMeter = gaugeMeterFactory.createUnblockedEventStreamsGaugeMeter(
                source,
                component);

        assertThat(getValueOfField(unblockedEventStreamsGaugeMeter, "source", String.class), is(source));
        assertThat(getValueOfField(unblockedEventStreamsGaugeMeter, "component", String.class), is(component));
        assertThat(getValueOfField(unblockedEventStreamsGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
    }

    @Test
    public void shouldCreateNewUpToDateEventStreamsGaugeMeter() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final UpToDateEventStreamsGaugeMeter upToDateEventStreamsGaugeMeter = gaugeMeterFactory.createUpToDateEventStreamsGaugeMeter(
                source,
                component);

        assertThat(getValueOfField(upToDateEventStreamsGaugeMeter, "source", String.class), is(source));
        assertThat(getValueOfField(upToDateEventStreamsGaugeMeter, "component", String.class), is(component));
        assertThat(getValueOfField(upToDateEventStreamsGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
    }
}