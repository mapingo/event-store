package uk.gov.justice.services.eventstore.management.replay.process;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;
import uk.gov.justice.services.subscription.SubscriptionManager;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        final String componentName = "componentName";
        final JsonEnvelope eventEnvelope = mock(JsonEnvelope.class);
        final SubscriptionManager subscriptionManager = mock(SubscriptionManager.class);
        when(subscriptionManagerSelector.selectFor(componentName)).thenReturn(subscriptionManager);

        transactionReplayEventProcessor.process(componentName, eventEnvelope);

        final InOrder inOrder = inOrder(micrometerMetricsCounters, subscriptionManager);

        inOrder.verify(micrometerMetricsCounters).incrementEventsReceivedCount();
        inOrder.verify(subscriptionManager).process(eventEnvelope);
    }
}