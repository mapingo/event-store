package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import uk.gov.justice.services.messaging.JsonEnvelope;

import org.junit.jupiter.api.Test;

public class NewSubscriptionManagerTest {

    private final NewSubscriptionManagerDelegate newSubscriptionManagerDelegate = mock(NewSubscriptionManagerDelegate.class);
    private final String componentName = "SOME-COMPONENT-NAME";

    private final NewSubscriptionManager newSubscriptionManager = new NewSubscriptionManager(
            newSubscriptionManagerDelegate,
            componentName
    );

    @Test
    public void shouldPassThroughToTheNewSubscriptionManagerDelegateWithTheComponentName() throws Exception {

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);

        newSubscriptionManager.process(incomingJsonEnvelope);

        verify(newSubscriptionManagerDelegate).process(incomingJsonEnvelope, componentName);
    }
}