package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_ALREADY_PROCESSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_OUT_OF_ORDER;

import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventOrderingStatusCalculatorTest {

    @InjectMocks
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;


    @Test
    public void shouldProcessEventIfIncomingPositionIsOneGreaterThanCurrentStreamPosition() throws Exception {

        final StreamUpdateContext streamUpdateContext_1 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_1.currentStreamPosition()).thenReturn(1L);
        when(streamUpdateContext_1.incomingEventPosition()).thenReturn(2L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_1), is(EVENT_CORRECTLY_ORDERED));

        final StreamUpdateContext streamUpdateContext_2 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_2.currentStreamPosition()).thenReturn(2L);
        when(streamUpdateContext_2.incomingEventPosition()).thenReturn(3L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_2), is(EVENT_CORRECTLY_ORDERED));

        final StreamUpdateContext streamUpdateContext_3 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_3.currentStreamPosition()).thenReturn(23L);
        when(streamUpdateContext_3.incomingEventPosition()).thenReturn(24L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_3), is(EVENT_CORRECTLY_ORDERED));
    }

    @Test
    public void shouldBufferEventIfIncomingPositionIsMoreThanOneGreaterThanCurrentStreamPosition() throws Exception {

        final StreamUpdateContext streamUpdateContext_1 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_1.currentStreamPosition()).thenReturn(1L);
        when(streamUpdateContext_1.incomingEventPosition()).thenReturn(3L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_1), is(EVENT_OUT_OF_ORDER));

        final StreamUpdateContext streamUpdateContext_2 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_2.currentStreamPosition()).thenReturn(2L);
        when(streamUpdateContext_2.incomingEventPosition()).thenReturn(8L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_2), is(EVENT_OUT_OF_ORDER));
    }

    @Test
    public void shouldIgnoreTheEventIfTheIncomingPositionIsLessThanOrEqualToTheCurrentStreamPosition() throws Exception {

        final StreamUpdateContext streamUpdateContext_1 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_1.currentStreamPosition()).thenReturn(10L);
        when(streamUpdateContext_1.incomingEventPosition()).thenReturn(10L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_1), is(EVENT_ALREADY_PROCESSED));

        final StreamUpdateContext streamUpdateContext_2 = mock(StreamUpdateContext.class);
        when(streamUpdateContext_2.currentStreamPosition()).thenReturn(12L);
        when(streamUpdateContext_2.incomingEventPosition()).thenReturn(11L);

        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext_2), is(EVENT_ALREADY_PROCESSED));
    }
}