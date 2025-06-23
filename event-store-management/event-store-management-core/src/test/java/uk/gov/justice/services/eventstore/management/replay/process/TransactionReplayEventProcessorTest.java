package uk.gov.justice.services.eventstore.management.replay.process;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;
import uk.gov.justice.services.subscription.SubscriptionManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionReplayEventProcessorTest {

    @Mock
    private SubscriptionManagerSelector subscriptionManagerSelector;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @InjectMocks
    private TransactionReplayEventProcessor transactionReplayEventProcessor;

    @Test
    void shouldCreateEventBufferProcessorAndInvokeProcess() {
        final String source = "some-source";
        final String component = "some-component";
        final JsonEnvelope eventEnvelope = mock(JsonEnvelope.class);
        final SubscriptionManager subscriptionManager = mock(SubscriptionManager.class);
        when(subscriptionManagerSelector.selectFor(component)).thenReturn(subscriptionManager);

        transactionReplayEventProcessor.process(source, component, eventEnvelope);

        final InOrder inOrder = inOrder(micrometerMetricsCounters, subscriptionManager);

        inOrder.verify(micrometerMetricsCounters).incrementEventsReceivedCount(source, component);
        inOrder.verify(subscriptionManager).process(eventEnvelope);
    }
}