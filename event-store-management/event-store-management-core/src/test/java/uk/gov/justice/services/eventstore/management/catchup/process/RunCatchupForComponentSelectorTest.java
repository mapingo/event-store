package uk.gov.justice.services.eventstore.management.catchup.process;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jmx.api.command.EventCatchupCommand;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RunCatchupForComponentSelectorTest {

    @Mock
    private CatchupTypeSelector catchupTypeSelector;

    @InjectMocks
    private RunCatchupForComponentSelector runCatchupForComponentSelector;

    @Test
    public void shouldRunIfRunningEventCatchupAndTheComponentIsEventListener() throws Exception {

        final String componentName = "EVENT_LISTENER";
        final EventCatchupCommand eventCatchupCommand = new EventCatchupCommand();

        when(catchupTypeSelector.isEventCatchup(componentName, eventCatchupCommand)).thenReturn(true);
        when(catchupTypeSelector.isIndexerCatchup(componentName, eventCatchupCommand)).thenReturn(false);

        final boolean shouldRun = runCatchupForComponentSelector.shouldRunForThisComponentAndType(componentName, eventCatchupCommand);

        assertThat(shouldRun, is(true));
    }

    @Test
    public void shouldRunIfRunningIndexCatchupAndTheComponentIsEventIndexer() throws Exception {

        final String componentName = "EVENT_INDEXER";
        final EventCatchupCommand eventCatchupCommand = new EventCatchupCommand();

        when(catchupTypeSelector.isEventCatchup(componentName, eventCatchupCommand)).thenReturn(true);
        when(catchupTypeSelector.isIndexerCatchup(componentName, eventCatchupCommand)).thenReturn(false);

        final boolean shouldRun = runCatchupForComponentSelector.shouldRunForThisComponentAndType(componentName, eventCatchupCommand);

        assertThat(shouldRun, is(true));
    }

    @Test
    public void shouldNotRunIfRunningIfNeitherComponentShouldRun() throws Exception {

        final String componentName = "EVENT_PROCESSOR";
        final EventCatchupCommand eventCatchupCommand = new EventCatchupCommand();

        when(catchupTypeSelector.isEventCatchup(componentName, eventCatchupCommand)).thenReturn(false);
        when(catchupTypeSelector.isIndexerCatchup(componentName, eventCatchupCommand)).thenReturn(false);

        final boolean shouldRun = runCatchupForComponentSelector.shouldRunForThisComponentAndType(componentName, eventCatchupCommand);

        assertThat(shouldRun, is(false));
    }
}
