package uk.gov.justice.services.eventstore.management.untrigger.commands;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventstore.management.logging.MdcLogger;
import uk.gov.justice.services.eventstore.management.untrigger.process.EventLogTriggerManipulator;
import uk.gov.justice.services.jmx.api.command.AddTriggerCommand;
import uk.gov.justice.services.jmx.api.command.RemoveTriggerCommand;

import java.util.function.Consumer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class AddRemoveTriggerCommandHandlerTest {

    @Mock
    private EventLogTriggerManipulator eventLogTriggerManipulator;

    @Mock
    private MdcLogger mdcLogger;

    @Mock
    private Logger logger;

    @InjectMocks
    private AddRemoveTriggerCommandHandler addRemoveTriggerCommandHandler;

    private Consumer<Runnable> testConsumer = Runnable::run;

    @Test
    public void shouldCallTheAddEventLogTriggerProcess() throws Exception {

        final AddTriggerCommand addTriggerCommand = new AddTriggerCommand();

        when(mdcLogger.mdcLoggerConsumer()).thenReturn(testConsumer);

        addRemoveTriggerCommandHandler.addTriggerToEventLogTable(addTriggerCommand);

        verify(logger).info("Received command ADD_TRIGGER");
        verify(eventLogTriggerManipulator).addTriggerToEventLogTable();
    }

    @Test
    public void shouldCallTheRemoveEventLogTriggerProcess() throws Exception {

        final RemoveTriggerCommand removeTriggerCommand = new RemoveTriggerCommand();

        when(mdcLogger.mdcLoggerConsumer()).thenReturn(testConsumer);

        addRemoveTriggerCommandHandler.removeTriggerFromEventLogTable(removeTriggerCommand);

        verify(logger).info("Received command REMOVE_TRIGGER");
        verify(eventLogTriggerManipulator).removeTriggerFromEventLogTable();
    }
}
