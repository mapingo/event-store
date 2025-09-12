package uk.gov.justice.services.eventsourcing.publishedevent.rebuild;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventsRebuilderTest {

    @Mock
    private EventNumberGetter eventNumberGetter;

    @Mock
    private BatchedPublishedEventInserterFactory batchedPublishedEventInserterFactory;

    @Mock
    private ActiveEventFilter activeEventFilter;

    @Mock
    private RebuildPublishedEventFactory rebuildPublishedEventFactory;

    @InjectMocks
    private PublishedEventsRebuilder publishedEventsRebuilder;

    @Test
    public void shouldConvertEventsToPublishedEventsAndInsertThemInTheDatabaseInBatches() throws Exception {

        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final UUID streamId_3 = randomUUID();

        final Event event_1 = mock(Event.class);
        final Event event_2 = mock(Event.class);
        final Event event_3 = mock(Event.class);

        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_3 = mock(LinkedEvent.class);

        final Stream<Event> eventStream = Stream.of(event_1, event_2, event_3);
        final AtomicLong currentEventNumber = new AtomicLong(1);
        final AtomicLong previousEventNumber = new AtomicLong(0);
        final Set<UUID> activeStreamIds = newHashSet(streamId_1, streamId_2, streamId_3);

        final BatchedPublishedEventInserter batchedPublishedEventInserter = mock(BatchedPublishedEventInserter.class);

        when(batchedPublishedEventInserterFactory.createInitialised()).thenReturn(batchedPublishedEventInserter);

        when(eventNumberGetter.eventNumberFrom(event_1)).thenReturn(1L);
        when(activeEventFilter.isActiveEvent(event_1, activeStreamIds)).thenReturn(true);
        when(rebuildPublishedEventFactory.createPublishedEventFrom(event_1, previousEventNumber)).thenReturn(linkedEvent_1);
        when(batchedPublishedEventInserter.addToBatch(linkedEvent_1)).thenReturn(linkedEvent_1);

        when(eventNumberGetter.eventNumberFrom(event_2)).thenReturn(2L);
        when(activeEventFilter.isActiveEvent(event_2, activeStreamIds)).thenReturn(true);
        when(rebuildPublishedEventFactory.createPublishedEventFrom(event_2, previousEventNumber)).thenReturn(linkedEvent_2);
        when(batchedPublishedEventInserter.addToBatch(linkedEvent_2)).thenReturn(linkedEvent_2);

        when(eventNumberGetter.eventNumberFrom(event_3)).thenReturn(3L);
        when(activeEventFilter.isActiveEvent(event_3, activeStreamIds)).thenReturn(true);
        when(rebuildPublishedEventFactory.createPublishedEventFrom(event_3, previousEventNumber)).thenReturn(linkedEvent_3);
        when(batchedPublishedEventInserter.addToBatch(linkedEvent_3)).thenReturn(linkedEvent_3);

        final List<LinkedEvent> linkedEvents = publishedEventsRebuilder.rebuild(
                eventStream,
                previousEventNumber, currentEventNumber,
                activeStreamIds);

        assertThat(linkedEvents.size(), is(3));
        assertThat(linkedEvents.get(0), is(linkedEvent_1));
        assertThat(linkedEvents.get(1), is(linkedEvent_2));
        assertThat(linkedEvents.get(2), is(linkedEvent_3));

        final InOrder inOrder = inOrder(batchedPublishedEventInserter);

        inOrder.verify(batchedPublishedEventInserter).addToBatch(linkedEvent_1);
        inOrder.verify(batchedPublishedEventInserter).addToBatch(linkedEvent_2);
        inOrder.verify(batchedPublishedEventInserter).addToBatch(linkedEvent_3);
        inOrder.verify(batchedPublishedEventInserter).insertBatch();
        inOrder.verify(batchedPublishedEventInserter).close();
    }

    @Test
    public void shouldOnlyInsertActiveEvents() throws Exception {

        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final UUID streamId_3 = randomUUID();

        final Event event_1 = mock(Event.class);
        final Event event_2 = mock(Event.class);
        final Event event_3 = mock(Event.class);

        final LinkedEvent linkedEvent_1 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_3 = mock(LinkedEvent.class);

        final Stream<Event> eventStream = Stream.of(event_1, event_2, event_3);
        final AtomicLong currentEventNumber = new AtomicLong(1);
        final AtomicLong previousEventNumber = new AtomicLong(0);
        final Set<UUID> activeStreamIds = newHashSet(streamId_1, streamId_2, streamId_3);

        final BatchedPublishedEventInserter batchedPublishedEventInserter = mock(BatchedPublishedEventInserter.class);

        when(batchedPublishedEventInserterFactory.createInitialised()).thenReturn(batchedPublishedEventInserter);

        when(eventNumberGetter.eventNumberFrom(event_1)).thenReturn(1L);
        when(activeEventFilter.isActiveEvent(event_1, activeStreamIds)).thenReturn(true);
        when(rebuildPublishedEventFactory.createPublishedEventFrom(event_1, previousEventNumber)).thenReturn(linkedEvent_1);
        when(batchedPublishedEventInserter.addToBatch(linkedEvent_1)).thenReturn(linkedEvent_1);

        when(eventNumberGetter.eventNumberFrom(event_2)).thenReturn(2L);
        when(activeEventFilter.isActiveEvent(event_2, activeStreamIds)).thenReturn(false);

        when(eventNumberGetter.eventNumberFrom(event_3)).thenReturn(3L);
        when(activeEventFilter.isActiveEvent(event_3, activeStreamIds)).thenReturn(true);
        when(rebuildPublishedEventFactory.createPublishedEventFrom(event_3, previousEventNumber)).thenReturn(linkedEvent_3);
        when(batchedPublishedEventInserter.addToBatch(linkedEvent_3)).thenReturn(linkedEvent_3);

        final List<LinkedEvent> linkedEvents = publishedEventsRebuilder.rebuild(
                eventStream,
                previousEventNumber, currentEventNumber,
                activeStreamIds);

        assertThat(linkedEvents.size(), is(2));
        assertThat(linkedEvents.get(0), is(linkedEvent_1));
        assertThat(linkedEvents.get(1), is(linkedEvent_3));

        final InOrder inOrder = inOrder(batchedPublishedEventInserter);

        inOrder.verify(batchedPublishedEventInserter).addToBatch(linkedEvent_1);
        inOrder.verify(batchedPublishedEventInserter).addToBatch(linkedEvent_3);
        inOrder.verify(batchedPublishedEventInserter).insertBatch();
        inOrder.verify(batchedPublishedEventInserter).close();
    }
}
