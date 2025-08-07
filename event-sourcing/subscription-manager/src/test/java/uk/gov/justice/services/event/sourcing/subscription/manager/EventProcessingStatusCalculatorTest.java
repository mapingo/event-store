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
public class EventProcessingStatusCalculatorTest {

    @InjectMocks
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;

    @Test
    public void shouldCalculateEventCorrectlyOrderedIfTheIncomingEventIsContiguousToCurrentStreamPosition() throws Exception {

        final long incomingEventPosition = 24;
        final long currentStreamPosition = 23;

        final StreamUpdateContext streamUpdateContext = mock(StreamUpdateContext.class);

        when(streamUpdateContext.incomingEventPosition()).thenReturn(incomingEventPosition);
        when(streamUpdateContext.currentStreamPosition()).thenReturn(currentStreamPosition);


        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext), is(EVENT_CORRECTLY_ORDERED));
    }

    @Test
    public void shouldCalculateEventOutOfOrderIfTheIncomingEventIsNotContiguousToCurrentStreamPosition() throws Exception {

        final long incomingEventPosition = 26;
        final long currentStreamPosition = 23;

        final StreamUpdateContext streamUpdateContext = mock(StreamUpdateContext.class);

        when(streamUpdateContext.incomingEventPosition()).thenReturn(incomingEventPosition);
        when(streamUpdateContext.currentStreamPosition()).thenReturn(currentStreamPosition);


        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext), is(EVENT_OUT_OF_ORDER));
    }

    @Test
    public void shouldCalculateEventAlreadyProcessIfTheIncomingEventIsLessThanTheCurrentStreamPosition() throws Exception {

        final long incomingEventPosition = 22;
        final long currentStreamPosition = 23;

        final StreamUpdateContext streamUpdateContext = mock(StreamUpdateContext.class);

        when(streamUpdateContext.incomingEventPosition()).thenReturn(incomingEventPosition);
        when(streamUpdateContext.currentStreamPosition()).thenReturn(currentStreamPosition);


        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext), is(EVENT_ALREADY_PROCESSED));
    }

    @Test
    public void shouldCalculateEventAlreadyProcessIfTheIncomingEventEqualToTheCurrentStreamPosition() throws Exception {

        final long incomingEventPosition = 23;
        final long currentStreamPosition = 23;

        final StreamUpdateContext streamUpdateContext = mock(StreamUpdateContext.class);

        when(streamUpdateContext.incomingEventPosition()).thenReturn(incomingEventPosition);
        when(streamUpdateContext.currentStreamPosition()).thenReturn(currentStreamPosition);


        assertThat(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamUpdateContext), is(EVENT_ALREADY_PROCESSED));
    }
}