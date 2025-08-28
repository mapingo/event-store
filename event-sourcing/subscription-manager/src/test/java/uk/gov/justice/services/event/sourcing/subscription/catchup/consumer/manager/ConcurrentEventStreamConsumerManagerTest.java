package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.ConsumeEventQueueTaskManager;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.EventQueueConsumer;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand;

import java.util.List;
import java.util.Queue;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConcurrentEventStreamConsumerManagerTest {

    @Mock
    private EventsInProcessCounterProvider eventsInProcessCounterProvider;

    @Mock
    private ConsumeEventQueueTaskManager consumeEventQueueTaskManager;

    @Spy
    private EventStreamsInProgressList eventStreamsInProgressList = new EventStreamsInProgressList();

    @InjectMocks
    private ConcurrentEventStreamConsumerManager concurrentEventStreamConsumerManager;

    @Captor
    private ArgumentCaptor<Queue<LinkedEvent>> eventQueueCaptor;

    @Test
    public void shouldCreateQueueAndCreateTaskToConsumeQueueForNewStreamId() {

        final CatchupCommand catchupCommand = new EventCatchupCommand();
        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final UUID streamId = randomUUID();
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);

        final EventQueueConsumer eventQueueConsumer = mock(EventQueueConsumer.class);
        final EventsInProcessCounter eventsInProcessCounter = mock(EventsInProcessCounter.class);

        when(eventsInProcessCounterProvider.getInstance()).thenReturn(eventsInProcessCounter);
        when(eventsInProcessCounter.maxNumberOfEventsInProcess()).thenReturn(false);
        when(linkedEvent.getStreamId()).thenReturn(streamId);

        concurrentEventStreamConsumerManager.add(linkedEvent, subscriptionName, catchupCommand, commandId);

        verify(consumeEventQueueTaskManager).consume(eventQueueCaptor.capture(), eq(subscriptionName), eq(catchupCommand), eq(commandId));

        final Queue<LinkedEvent> events = eventQueueCaptor.getValue();
        assertThat(events.size(), is(1));
        assertThat(events.poll(), is(linkedEvent));

        verify(eventsInProcessCounter).incrementEventsInProcessCount();
    }

    @Test
    public void shouldNotCreateQueueOrCreateTaskIfEventIsSameStreamId() {

        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final UUID streamId = randomUUID();
        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final EventQueueConsumer eventQueueConsumer = mock(EventQueueConsumer.class);
        final CatchupCommand catchupCommand = new EventCatchupCommand();

        final EventsInProcessCounter eventsInProcessCounter = mock(EventsInProcessCounter.class);

        when(eventsInProcessCounterProvider.getInstance()).thenReturn(eventsInProcessCounter);
        when(eventsInProcessCounter.maxNumberOfEventsInProcess()).thenReturn(false);
        when(linkedEvent_1.getStreamId()).thenReturn(streamId);
        when(linkedEvent_2.getStreamId()).thenReturn(streamId);

        concurrentEventStreamConsumerManager.add(linkedEvent_1, subscriptionName, catchupCommand, commandId);
        concurrentEventStreamConsumerManager.add(linkedEvent_2, subscriptionName, catchupCommand, commandId);

        verify(consumeEventQueueTaskManager).consume(eventQueueCaptor.capture(), eq(subscriptionName), eq(catchupCommand), eq(commandId));

        final Queue<LinkedEvent> eventsStream = eventQueueCaptor.getValue();
        assertThat(eventsStream.size(), is(2));
        assertThat(eventsStream.poll(), is(linkedEvent_1));
        assertThat(eventsStream.poll(), is(linkedEvent_2));

        verify(eventsInProcessCounter, times(2)).incrementEventsInProcessCount();
    }

    @Test
    public void shouldCreateQueueForEachStreamId() {

        final CatchupCommand catchupCommand = new EventCatchupCommand();
        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final EventQueueConsumer eventQueueConsumer = mock(EventQueueConsumer.class);

        final EventsInProcessCounter eventsInProcessCounter = mock(EventsInProcessCounter.class);

        when(eventsInProcessCounterProvider.getInstance()).thenReturn(eventsInProcessCounter);
        when(eventsInProcessCounter.maxNumberOfEventsInProcess()).thenReturn(false);
        when(linkedEvent_1.getStreamId()).thenReturn(streamId_1);
        when(linkedEvent_2.getStreamId()).thenReturn(streamId_2);

        concurrentEventStreamConsumerManager.add(linkedEvent_1, subscriptionName, catchupCommand, commandId);
        concurrentEventStreamConsumerManager.add(linkedEvent_2, subscriptionName, catchupCommand, commandId);

        verify(consumeEventQueueTaskManager, times(2)).consume(eventQueueCaptor.capture(), eq(subscriptionName), eq(catchupCommand), eq(commandId));

        final List<Queue<LinkedEvent>> allValues = eventQueueCaptor.getAllValues();

        final Queue<LinkedEvent> eventsStream_1 = allValues.get(0);
        assertThat(eventsStream_1.size(), is(1));
        assertThat(eventsStream_1.poll(), is(linkedEvent_1));

        final Queue<LinkedEvent> eventsStream_2 = allValues.get(1);
        assertThat(eventsStream_2.size(), is(1));
        assertThat(eventsStream_2.poll(), is(linkedEvent_2));

        verify(eventsInProcessCounter, times(2)).incrementEventsInProcessCount();
    }

    @Test
    public void shouldBeAbleToFinishQueueAndAllowAnotherProcessToPickupQueueIfNotEmpty() {

        final CatchupCommand catchupCommand = new EventCatchupCommand();
        final UUID commandId = randomUUID();
        final String subscriptionName = "subscriptionName";
        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final EventQueueConsumer eventQueueConsumer = mock(EventQueueConsumer.class);

        final EventsInProcessCounter eventsInProcessCounter = mock(EventsInProcessCounter.class);

        when(eventsInProcessCounterProvider.getInstance()).thenReturn(eventsInProcessCounter);
        when(eventsInProcessCounter.maxNumberOfEventsInProcess()).thenReturn(false);
        when(linkedEvent_1.getStreamId()).thenReturn(streamId_1);
        when(linkedEvent_2.getStreamId()).thenReturn(streamId_2);

        concurrentEventStreamConsumerManager.add(linkedEvent_1, subscriptionName, catchupCommand, commandId);

        verify(consumeEventQueueTaskManager).consume(eventQueueCaptor.capture(), eq(subscriptionName), eq(catchupCommand), eq(commandId));

        final Queue<LinkedEvent> eventsStream_1 = eventQueueCaptor.getValue();
        assertThat(eventsStream_1.size(), is(1));
        assertThat(eventsStream_1.poll(), is(linkedEvent_1));

        concurrentEventStreamConsumerManager.isEventConsumptionComplete(new FinishedProcessingMessage(eventsStream_1));
        concurrentEventStreamConsumerManager.add(linkedEvent_2, subscriptionName, catchupCommand, commandId);

        verify(consumeEventQueueTaskManager, times(2)).consume(eventQueueCaptor.capture(), eq(subscriptionName), eq(catchupCommand), eq(commandId));

        final Queue<LinkedEvent> eventsStream_2 = eventQueueCaptor.getValue();
        assertThat(eventsStream_2.size(), is(1));
        assertThat(eventsStream_2.poll(), is(linkedEvent_2));

        verify(eventsInProcessCounter, times(2)).incrementEventsInProcessCount();
    }

    @Test
    public void shouldBlockOnTheEventsStreamInProgressListWhenWaitingForCompletion() throws Exception {

        concurrentEventStreamConsumerManager.waitForCompletion();

        verify(eventStreamsInProgressList).blockUntilEmpty();
    }

    @Test
    public void shouldDecrementTheEventsInProcessCount() throws Exception {

        final EventsInProcessCounter eventsInProcessCounter = mock(EventsInProcessCounter.class);

        when(eventsInProcessCounterProvider.getInstance()).thenReturn(eventsInProcessCounter);

        concurrentEventStreamConsumerManager.decrementEventsInProcessCount();

        verify(eventsInProcessCounter).decrementEventsInProcessCount();
    }

    @Test
    public void shouldDecrementTheEventsInProcessCountByGivenNumber() throws Exception {

        final int count = 2;
        final EventsInProcessCounter eventsInProcessCounter = mock(EventsInProcessCounter.class);

        when(eventsInProcessCounterProvider.getInstance()).thenReturn(eventsInProcessCounter);

        concurrentEventStreamConsumerManager.decrementEventsInProcessCountBy(count);

        verify(eventsInProcessCounter).decrementEventsInProcessCountBy(count);
    }
}
