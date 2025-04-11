package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;

public class NewSubscriptionManager implements SubscriptionManager  {

    private final NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;
    private final String componentName;

    public NewSubscriptionManager(final NewSubscriptionManagerDelegate newSubscriptionManagerDelegate, final String componentName) {
        this.newSubscriptionManagerDelegate = newSubscriptionManagerDelegate;
        this.componentName = componentName;
    }

    @Override
    public void process(final JsonEnvelope incomingJsonEnvelope) {
        newSubscriptionManagerDelegate.process(incomingJsonEnvelope, componentName);
    }
}
