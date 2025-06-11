package uk.gov.justice.services.eventstore.metrics.tags;

import static java.util.Collections.unmodifiableMap;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.COMPONENT_TAG_NAME;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.SOURCE_TAG_NAME;

import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import io.micrometer.core.instrument.Tag;

public class TagProvider {

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    private Map<Tag, List<Tag>> tagMapByComponentAndSourceList;

    public synchronized Map<Tag, List<Tag>> getComponentTags() {

        if(tagMapByComponentAndSourceList == null) {
            tagMapByComponentAndSourceList = createComponentTagsMap();
        }

        return tagMapByComponentAndSourceList;
    }

    private Map<Tag, List<Tag>> createComponentTagsMap() {

        final Map<Tag, List<Tag>> tagsMap = new HashMap<>();
        final List<SubscriptionsDescriptor> subscriptionsDescriptors = subscriptionsDescriptorsRegistry.getAll();

        subscriptionsDescriptors.forEach(subscriptionsDescriptor -> {
            final String serviceComponent = subscriptionsDescriptor.getServiceComponent();
            final List<Tag> eventSourceTags = subscriptionsDescriptor.getSubscriptions().stream()
                    .map(subscription -> Tag.of(SOURCE_TAG_NAME.getTagName(), subscription.getEventSourceName()))
                    .toList();
            tagsMap.put(Tag.of(COMPONENT_TAG_NAME.getTagName(), serviceComponent), eventSourceTags);
        });

        return unmodifiableMap(tagsMap);
    }
}
