package uk.gov.justice.services.eventsourcing.prepublish;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static uk.gov.justice.services.messaging.spi.DefaultJsonMetadata.metadataBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class LinkedEventFactoryTest {

    @InjectMocks
    private LinkedEventFactory linkedEventFactory;


    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void shouldCreateALinkedEventFromAnEvent() throws Exception {

        final long eventNumber = 923874L;
        final long previousEventNumber = 923873L;

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final long sequenceId = 23487L;
        final String name = "event-name";
        final String payload = "payload";
        final ZonedDateTime createdAt = new UtcClock().now();

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
                sequenceId,
                name,
                "some metadata",
                payload,
                createdAt
        );


        final LinkedEvent linkedEvent = linkedEventFactory.create(
                event,
                updatedMetadata,
                eventNumber,
                previousEventNumber);

        assertThat(linkedEvent.getId(), is(eventId));
        assertThat(linkedEvent.getStreamId(), is(streamId));
        assertThat(linkedEvent.getSequenceId(), is(sequenceId));
        assertThat(linkedEvent.getName(), is(name));
        assertThat(linkedEvent.getPayload(), is(payload));
        assertThat(linkedEvent.getMetadata(), is(updatedMetadata.asJsonObject().toString()));
        assertThat(linkedEvent.getCreatedAt(), is(createdAt));
        assertThat(linkedEvent.getEventNumber().get(), is(eventNumber));
        assertThat(linkedEvent.getPreviousEventNumber(), is(previousEventNumber));
    }
}