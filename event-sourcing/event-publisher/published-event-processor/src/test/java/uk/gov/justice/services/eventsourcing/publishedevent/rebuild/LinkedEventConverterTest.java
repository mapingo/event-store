package uk.gov.justice.services.eventsourcing.publishedevent.rebuild;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventConverterTest {

    @Mock
    private MetadataEventNumberUpdater metadataEventNumberUpdater;

    @Mock
    private EventConverter eventConverter;

    @InjectMocks
    private PublishedEventConverter publishedEventConverter;

    @Test
    public void shouldConvertEventAndPreviousEventNumberToPublishedEvent() {

        final long eventNumber = 923874L;
        final long previousEventNumber = 923873L;

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final long positionInStream = 23487L;
        final String name = "event-name";
        final String payload = "payload";
        final ZonedDateTime createdAt = new UtcClock().now();

        final Metadata metadata = mock(Metadata.class);
        final Metadata updatedMetadata = metadataBuilder()
                .withId(eventId)
                .withName(name)
                .withStreamId(streamId)
                .withEventNumber(eventNumber)
                .withPreviousEventNumber(previousEventNumber)
                .build();

        final Event event = new Event(
                eventId,
                streamId,
                positionInStream,
                name,
                "some metadata",
                payload,
                createdAt,
                Optional.of(eventNumber)
        );

        when(eventConverter.metadataOf(event)).thenReturn(metadata);
        when(metadataEventNumberUpdater.updateMetadataJson(metadata, previousEventNumber, eventNumber)).thenReturn(updatedMetadata);

        final LinkedEvent linkedEvent = publishedEventConverter.toPublishedEvent(event, previousEventNumber);

        assertThat(linkedEvent.getId(), is(eventId));
        assertThat(linkedEvent.getStreamId(), is(streamId));
        assertThat(linkedEvent.getPositionInStream(), is(positionInStream));
        assertThat(linkedEvent.getName(), is(name));
        assertThat(linkedEvent.getPayload(), is(payload));
        assertThat(linkedEvent.getMetadata(), is(updatedMetadata.asJsonObject().toString()));
        assertThat(linkedEvent.getCreatedAt(), is(createdAt));
        assertThat(linkedEvent.getEventNumber().get(), is(eventNumber));
        assertThat(linkedEvent.getPreviousEventNumber(), is(previousEventNumber));
    }

    @Test
    public void shouldThrowExceptionIfEventNumberIsNotPresent() {

        final long previousEventNumber = 923873L;
        final UUID eventId = randomUUID();
        final Event event = mock(Event.class);

        when(event.getId()).thenReturn(eventId);
        when(event.getEventNumber()).thenReturn(Optional.empty());

        try {
            publishedEventConverter.toPublishedEvent(event, previousEventNumber);
            fail();
        } catch (final RebuildException e) {
            assertThat(e.getMessage(), is(format("No event number found for event with id '%s'", eventId)));
        }
    }
}