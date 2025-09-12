package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.PublishedEventSourceProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.PublishedEventSource;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;
import uk.gov.justice.services.subscription.ProcessedEventTrackingService;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MissingEventStreamerTest {

    @Mock
    private PublishedEventSourceProvider publishedEventSourceProvider;

    @Mock
    private ProcessedEventTrackingService processedEventTrackingService;

    @InjectMocks
    private MissingEventStreamer missingEventStreamer;

    @Test
    public void shouldFindTheRangesOfMissingEventsAndStreamThemAsPublishedEvents() throws Exception {

        final String componentName = "EVENT_LISTENER";
        final String eventSourceName = "event source name";
        final Long highestPublishedEventNumber = 23L;

        final PublishedEventSource publishedEventSource = mock(PublishedEventSource.class);

        final MissingEventRange missingEventRange_1 = mock(MissingEventRange.class);
        final MissingEventRange missingEventRange_2 = mock(MissingEventRange.class);

        final Stream<MissingEventRange> missingEventRangeStream = Stream.of(missingEventRange_1, missingEventRange_2);

        final LinkedEvent linkedEvent_2 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_3 = mock(LinkedEvent.class);

        final LinkedEvent linkedEvent_7 = mock(LinkedEvent.class);
        final LinkedEvent linkedEvent_8 = mock(LinkedEvent.class);

        final Stream<LinkedEvent> publishedEventStream_1 = Stream.of(linkedEvent_2, linkedEvent_3);
        final Stream<LinkedEvent> publishedEventStream_2 = Stream.of(linkedEvent_7, linkedEvent_8);


        when(publishedEventSourceProvider.getPublishedEventSource(eventSourceName)).thenReturn(publishedEventSource);
        when(publishedEventSource.getHighestPublishedEventNumber()).thenReturn(highestPublishedEventNumber);
        when(processedEventTrackingService.getAllMissingEvents(eventSourceName, componentName, highestPublishedEventNumber)).thenReturn(missingEventRangeStream);
        when(publishedEventSource.findEventRange(missingEventRange_1)).thenReturn(publishedEventStream_1);
        when(publishedEventSource.findEventRange(missingEventRange_2)).thenReturn(publishedEventStream_2);

        final List<LinkedEvent> missingEvents = missingEventStreamer.getMissingEvents(eventSourceName, componentName)
                .collect(toList());

        assertThat(missingEvents.size(), is(4));
        assertThat(missingEvents.get(0), is(linkedEvent_2));
        assertThat(missingEvents.get(1), is(linkedEvent_3));
        assertThat(missingEvents.get(2), is(linkedEvent_7));
        assertThat(missingEvents.get(3), is(linkedEvent_8));
    }
}
