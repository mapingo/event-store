package uk.gov.justice.services.resources;

import java.util.Set;
import org.junit.jupiter.api.Test;
import uk.gov.justice.services.resources.rest.streams.StreamErrorsResource;
import uk.gov.justice.services.resources.rest.streams.StreamsResource;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class FrameworkApplicationTest {

    private final FrameworkApplication frameworkApplication = new FrameworkApplication();

    @Test
    void validateConfiguredResourceClasses() {
        final Set<Class<?>> resourceClasses = frameworkApplication.getClasses();

        assertThat(resourceClasses.size(), is(2));
        assertThat(resourceClasses, hasItems(StreamsResource.class, StreamErrorsResource.class));
    }
}