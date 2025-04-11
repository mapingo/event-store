package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.BackwardsCompatibleSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.DefaultSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManager;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SubscriptionManagerSelectorTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionDescriptorRegistry;

    @Mock
    private DefaultSubscriptionManagerFactory defaultSubscriptionManagerFactory;

    @Mock
    private BackwardsCompatibleSubscriptionManagerFactory backwardsCompatibleSubscriptionManagerFactory;

    @Mock
    private EventErrorHandlingConfiguration eventErrorHandlingConfiguration;

    @Mock
    private NewSubscriptionManagerFactory newSubscriptionManagerFactory;

    @InjectMocks
    private SubscriptionManagerSelector subscriptionManagerSelector;

    @Test
    public void shouldCreateDefaultSubscriptionManagerIfTheComponentIsAnEventListener() throws Exception {

        final String subscriptionName = "subscriptionName";
        final String componentName = "MY_EVENT_LISTENER";

        final Subscription subscription = mock(Subscription.class);

        final DefaultSubscriptionManager defaultSubscriptionManager = mock(DefaultSubscriptionManager.class);

        when(eventErrorHandlingConfiguration.isEventErrorHandlingEnabled()).thenReturn(false);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscriptionDescriptorRegistry.findComponentNameBy(subscriptionName)).thenReturn(componentName);
        when(defaultSubscriptionManagerFactory.create(componentName)).thenReturn(defaultSubscriptionManager);

        assertThat(subscriptionManagerSelector.selectFor(subscription), is(defaultSubscriptionManager));

        verifyNoInteractions(backwardsCompatibleSubscriptionManagerFactory);
        verifyNoInteractions(newSubscriptionManagerFactory);
    }

    @Test
    public void shouldCreateNewSubscriptionManagerIfTheComponentIsAnEventListenerAndErrorHandlingIsEnabled() throws Exception {

        final String subscriptionName = "subscriptionName";
        final String componentName = "MY_EVENT_LISTENER";

        final Subscription subscription = mock(Subscription.class);

        final NewSubscriptionManager newSubscriptionManager = mock(NewSubscriptionManager.class);

        when(eventErrorHandlingConfiguration.isEventErrorHandlingEnabled()).thenReturn(true);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscriptionDescriptorRegistry.findComponentNameBy(subscriptionName)).thenReturn(componentName);
        when(newSubscriptionManagerFactory.create(componentName)).thenReturn(newSubscriptionManager);

        assertThat(subscriptionManagerSelector.selectFor(subscription), is(newSubscriptionManager));

        verifyNoInteractions(backwardsCompatibleSubscriptionManagerFactory);
        verifyNoInteractions(defaultSubscriptionManagerFactory);
    }

    @Test
    public void shouldCreateDefaultSubscriptionManagerIfTheComponentIsAnEventIndexer() throws Exception {

        final String subscriptionName = "subscriptionName";
        final String componentName = "MY_EVENT_INDEXER";

        final Subscription subscription = mock(Subscription.class);

        final DefaultSubscriptionManager defaultSubscriptionManager = mock(DefaultSubscriptionManager.class);

        when(eventErrorHandlingConfiguration.isEventErrorHandlingEnabled()).thenReturn(false);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscriptionDescriptorRegistry.findComponentNameBy(subscriptionName)).thenReturn(componentName);
        when(defaultSubscriptionManagerFactory.create(componentName)).thenReturn(defaultSubscriptionManager);

        assertThat(subscriptionManagerSelector.selectFor(subscription), is(defaultSubscriptionManager));

        verifyNoInteractions(backwardsCompatibleSubscriptionManagerFactory);
        verifyNoInteractions(newSubscriptionManagerFactory);
    }

    @Test
    public void shouldCreateNewSubscriptionManagerIfTheComponentIsAnEventIndexerAndErrorHandlingIsEnabled() throws Exception {

        final String subscriptionName = "subscriptionName";
        final String componentName = "MY_EVENT_INDEXER";

        final Subscription subscription = mock(Subscription.class);

        final NewSubscriptionManager newSubscriptionManager = mock(NewSubscriptionManager.class);

        when(eventErrorHandlingConfiguration.isEventErrorHandlingEnabled()).thenReturn(true);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscriptionDescriptorRegistry.findComponentNameBy(subscriptionName)).thenReturn(componentName);
        when(eventErrorHandlingConfiguration.isEventErrorHandlingEnabled()).thenReturn(true);
        when(newSubscriptionManagerFactory.create(componentName)).thenReturn(newSubscriptionManager);

        assertThat(subscriptionManagerSelector.selectFor(subscription), is(newSubscriptionManager));

        verifyNoInteractions(backwardsCompatibleSubscriptionManagerFactory);
        verifyNoInteractions(defaultSubscriptionManagerFactory);
    }

    @Test
    public void shouldCreateBackwardsCompatibleSubscriptionManagerIfTheComponentIsNotAnEventListener() throws Exception {

        final String subscriptionName = "subscriptionName";
        final String componentName = "MY_EVENT_PROCESSOR";

        final Subscription subscription = mock(Subscription.class);

        final BackwardsCompatibleSubscriptionManager backwardsCompatibleSubscriptionManager = mock(BackwardsCompatibleSubscriptionManager.class);

        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscriptionDescriptorRegistry.findComponentNameBy(subscriptionName)).thenReturn(componentName);
        when(backwardsCompatibleSubscriptionManagerFactory.create(componentName)).thenReturn(backwardsCompatibleSubscriptionManager);

        assertThat(subscriptionManagerSelector.selectFor(subscription), is(backwardsCompatibleSubscriptionManager));

        verifyNoInteractions(defaultSubscriptionManagerFactory);
        verifyNoInteractions(newSubscriptionManagerFactory);
    }
}
