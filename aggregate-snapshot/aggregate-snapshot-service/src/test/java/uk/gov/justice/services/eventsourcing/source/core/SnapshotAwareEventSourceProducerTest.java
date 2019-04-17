package uk.gov.justice.services.eventsourcing.source.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.subscription.domain.builders.EventSourceDefinitionBuilder.eventSourceDefinition;

import uk.gov.justice.services.eventsourcing.source.core.annotation.EventSourceName;
import uk.gov.justice.subscription.domain.eventsource.EventSourceDefinition;
import uk.gov.justice.subscription.domain.eventsource.Location;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;

import javax.enterprise.inject.CreationException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SnapshotAwareEventSourceProducerTest {

    @Mock
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Mock
    private SnapshotAwareEventSourceFactory snapshotAwareEventSourceFactory;

    @InjectMocks
    private SnapshotAwareEventSourceProducer snapshotAwareEventSourceProducer;

    @Test
    public void shouldCreateDefaultEventSourceDefinitionWhenEventSourceNameIsEmpty() throws Exception {

        final String jndiDataSourceName = "jndiDataSourceName";
        final EventSourceDefinition eventSourceDefinition = eventSourceDefinition()
                .withName("defaultEventSource")
                .withDefault(true)
                .withLocation(new Location("", "", Optional.of(jndiDataSourceName)))
                .build();
        final EventSourceName eventSourceNameAnnotation = mock(EventSourceName.class);
        final JdbcBasedEventSource jdbcBasedEventSource = mock(JdbcBasedEventSource.class);

        when(eventSourceNameAnnotation.value()).thenReturn("");
        when(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition()).thenReturn(eventSourceDefinition);
        when(snapshotAwareEventSourceFactory.create(eventSourceDefinition.getName())).thenReturn(jdbcBasedEventSource);

        assertThat(snapshotAwareEventSourceProducer.eventSource(), is(jdbcBasedEventSource));
    }

    @Test
    public void shouldCreateDefaultEventSourceWhenNoEventSourceNameQualifierSet() throws Exception {

        final String jndiDataSourceName = "jndiDataSourceName";
        final EventSourceDefinition eventSourceDefinition = eventSourceDefinition()
                .withName("defaultEventSource")
                .withDefault(true)
                .withLocation(new Location("", "", Optional.of(jndiDataSourceName)))
                .build();
        final JdbcBasedEventSource jdbcBasedEventSource = mock(JdbcBasedEventSource.class);

        when(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition()).thenReturn(eventSourceDefinition);
        when(snapshotAwareEventSourceFactory.create(eventSourceDefinition.getName())).thenReturn(jdbcBasedEventSource);

        assertThat(snapshotAwareEventSourceProducer.eventSource(), is(jdbcBasedEventSource));
    }

    @Test
    public void shouldFailIfNoDataSourceNameFoundInEventSourcesYaml() throws Exception {

        final EventSourceDefinition eventSourceDefinition = eventSourceDefinition()
                .withName("defaultEventSource")
                .withDefault(true)
                .withLocation(new Location("", "", Optional.empty()))
                .build();
        final EventSourceName eventSourceNameAnnotation = mock(EventSourceName.class);
        final JdbcBasedEventSource jdbcBasedEventSource = mock(JdbcBasedEventSource.class);

        when(eventSourceNameAnnotation.value()).thenReturn("");
        when(eventSourceDefinitionRegistry.getDefaultEventSourceDefinition()).thenReturn(eventSourceDefinition);
        when(snapshotAwareEventSourceFactory.create(eventSourceDefinition.getName())).thenReturn(jdbcBasedEventSource);

        try {
            snapshotAwareEventSourceProducer.eventSource();
            fail();
        } catch (final CreationException expected) {
            assertThat(expected.getMessage(), is("No DataSource specified for EventSource 'defaultEventSource' specified in event-sources.yaml"));
        }

        verifyZeroInteractions(snapshotAwareEventSourceFactory);
    }
}
