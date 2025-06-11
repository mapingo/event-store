package uk.gov.justice.services.eventstore.metrics.tags;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.COMPONENT_TAG_NAME;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.SOURCE_TAG_NAME;

import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TagProviderTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @InjectMocks
    private TagProvider tagProvider;

    @Test
    public void shouldGetTheMapOfComponentAndSourceNamesFromTheSubscriptionRegistryAsMicrometerTags() throws Exception {

        final String component_1 = "some-component-1";
        final String component_2 = "some-component-2";

        final String source_1_1 = "source-1-1";
        final String source_1_2 = "source-1-2";
        final String source_2_1 = "source-2-1";
        final String source_2_2 = "source-2-2";

        final SubscriptionsDescriptor subscriptionsDescriptor_1 = mock(SubscriptionsDescriptor.class);
        final SubscriptionsDescriptor subscriptionsDescriptor_2 = mock(SubscriptionsDescriptor.class);

        final Subscription subscription_1_1 = mock(Subscription.class);
        final Subscription subscription_1_2 = mock(Subscription.class);
        final Subscription subscription_2_1 = mock(Subscription.class);
        final Subscription subscription_2_2 = mock(Subscription.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(List.of(subscriptionsDescriptor_1, subscriptionsDescriptor_2));
        when(subscriptionsDescriptor_1.getServiceComponent()).thenReturn(component_1);
        when(subscriptionsDescriptor_1.getSubscriptions()).thenReturn(List.of(subscription_1_1, subscription_1_2));
        when(subscriptionsDescriptor_2.getServiceComponent()).thenReturn(component_2);
        when(subscriptionsDescriptor_2.getSubscriptions()).thenReturn(List.of(subscription_2_1, subscription_2_2));
        when(subscription_1_1.getEventSourceName()).thenReturn(source_1_1);
        when(subscription_1_2.getEventSourceName()).thenReturn(source_1_2);
        when(subscription_2_1.getEventSourceName()).thenReturn(source_2_1);
        when(subscription_2_2.getEventSourceName()).thenReturn(source_2_2);

        final Map<Tag, List<Tag>> componentTags = tagProvider.getComponentTags();

        assertThat(componentTags.size(), is(2));

        final List<Tag> sourceTags_1 = componentTags.get(Tag.of(COMPONENT_TAG_NAME.getTagName(), component_1));
        assertThat(sourceTags_1.size(), is(2));
        assertThat(sourceTags_1.get(0), is(Tag.of(SOURCE_TAG_NAME.getTagName(), source_1_1)));
        assertThat(sourceTags_1.get(1), is(Tag.of(SOURCE_TAG_NAME.getTagName(), source_1_2)));

        final List<Tag> sourceTags_2 = componentTags.get(Tag.of("component", component_2));
        assertThat(sourceTags_2.size(), is(2));
        assertThat(sourceTags_2.get(0), is(Tag.of(SOURCE_TAG_NAME.getTagName(), source_2_1)));
        assertThat(sourceTags_2.get(1), is(Tag.of(SOURCE_TAG_NAME.getTagName(), source_2_2)));
    }

    @Test
    public void shouldCreateTheTagMapOnlyOnce() throws Exception {

        final String component = "some-component";
        final String source = "source";

        final SubscriptionsDescriptor subscriptionsDescriptor = mock(SubscriptionsDescriptor.class);
        final Subscription subscription = mock(Subscription.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(List.of(subscriptionsDescriptor));
        when(subscriptionsDescriptor.getServiceComponent()).thenReturn(component);
        when(subscriptionsDescriptor.getSubscriptions()).thenReturn(List.of(subscription));
        when(subscription.getEventSourceName()).thenReturn(source);

        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();
        tagProvider.getComponentTags();

        verify(subscriptionsDescriptorsRegistry, times(1)).getAll();
    }
}