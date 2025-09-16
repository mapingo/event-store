package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.test.utils.events.LinkedEventBuilder.publishedEventBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.test.utils.core.eventsource.EventStoreInitializer;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventQueriesIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    @InjectMocks
    private PublishedEventQueries publishedEventQueries;

    @BeforeEach
    public void initDatabase() throws Exception {
        new EventStoreInitializer().initializeEventStore(eventStoreDataSource);
    }

    @Test
    public void shouldInsertPublishedEvent() throws Exception {

        final LinkedEvent linkedEvent = new LinkedEvent(
                randomUUID(),
                randomUUID(),
                982347L,
                "an-event.name",
                "{\"some\": \"metadata\"}",
                "{\"the\": \"payload\"}",
                new UtcClock().now(),
                23L,
                22L
        );

        publishedEventQueries.insertPublishedEvent(linkedEvent, eventStoreDataSource);

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM published_event");
             final ResultSet resultSet = preparedStatement.executeQuery()) {


            if (resultSet.next()) {
                assertThat(resultSet.getObject(1), is(linkedEvent.getId()));
                assertThat(resultSet.getObject(2), is(linkedEvent.getStreamId()));
                assertThat(resultSet.getObject(3), is(linkedEvent.getPositionInStream()));
                assertThat(resultSet.getString(4), is(linkedEvent.getName()));
                assertThat(resultSet.getString(5), is(linkedEvent.getPayload()));
                assertThat(resultSet.getString(6), is(linkedEvent.getMetadata()));
                assertThat(fromSqlTimestamp(resultSet.getTimestamp(7)), is(linkedEvent.getCreatedAt()));
                assertThat(resultSet.getLong(8), is(linkedEvent.getEventNumber().get()));
                assertThat(resultSet.getObject(9), is(linkedEvent.getPreviousEventNumber()));
            } else {
                fail();
            }
        }
    }

    @Test
    public void shouldFetchPublishedEventById() throws Exception {

        final LinkedEvent linkedEvent = publishedEventBuilder()
                .withName("example.published-event")
                .withPositionInStream(1L)
                .withEventNumber(1L)
                .withPreviousEventNumber(0L)
                .build();

        publishedEventQueries.insertPublishedEvent(linkedEvent, eventStoreDataSource);

        final Optional<LinkedEvent> publishedEventOptional = publishedEventQueries.getPublishedEvent(
                linkedEvent.getId(),
                eventStoreDataSource);

        if (publishedEventOptional.isPresent()) {
            assertThat(publishedEventOptional.get(), is(linkedEvent));
        } else {
            fail();
        }
    }

    @Test
    public void shouldReturnEmptyIfNoPublishedEventFound() throws Exception {

        final UUID unknownId = randomUUID();

        assertThat(publishedEventQueries.getPublishedEvent(unknownId, eventStoreDataSource).isPresent(), is(false));
    }

    @Test
    public void shouldTruncatePublishedEventTable() throws Exception {

        final LinkedEvent linkedEvent = new LinkedEvent(
                randomUUID(),
                randomUUID(),
                982347L,
                "an-event.name",
                "{\"some\": \"metadata\"}",
                "{\"the\": \"payload\"}",
                new UtcClock().now(),
                23L,
                22L
        );

        publishedEventQueries.insertPublishedEvent(linkedEvent, eventStoreDataSource);
        publishedEventQueries.truncate(eventStoreDataSource);


        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM published_event");
             final ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                fail();
            }
        }
    }
}
