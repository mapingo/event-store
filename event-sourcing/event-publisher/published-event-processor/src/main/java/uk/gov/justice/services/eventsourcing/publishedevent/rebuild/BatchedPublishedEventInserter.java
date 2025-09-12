package uk.gov.justice.services.eventsourcing.publishedevent.rebuild;

import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.PublishedEventStatements.INSERT_INTO_PUBLISHED_EVENT_SQL;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.util.io.Closer;
import uk.gov.justice.services.jdbc.persistence.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

public class BatchedPublishedEventInserter implements AutoCloseable {

    private final Closer closer;

    private PreparedStatement preparedStatement;
    private Connection connection;

    public BatchedPublishedEventInserter(final Closer closer) {
        this.closer = closer;
    }

    public void prepareForInserts(final DataSource eventStoreDataSource) {
        try {
            connection = eventStoreDataSource.getConnection();
            preparedStatement = connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL);
        } catch (final SQLException e) {
            throw new DataAccessException("Failed to prepare statement for batch insert of PublishedEvents", e);
        }
    }

    public LinkedEvent addToBatch(final LinkedEvent linkedEvent) {

        try {
            preparedStatement.setObject(1, linkedEvent.getId());
            preparedStatement.setObject(2, linkedEvent.getStreamId());
            preparedStatement.setLong(3, linkedEvent.getPositionInStream());
            preparedStatement.setString(4, linkedEvent.getName());
            preparedStatement.setString(5, linkedEvent.getPayload());
            preparedStatement.setString(6, linkedEvent.getMetadata());
            preparedStatement.setObject(7, toSqlTimestamp(linkedEvent.getCreatedAt()));
            preparedStatement.setLong(8, linkedEvent.getEventNumber().orElseThrow(() -> new MissingEventNumberException("Event with id '%s' does not have an event number")));
            preparedStatement.setLong(9, linkedEvent.getPreviousEventNumber());

            preparedStatement.addBatch();

            return linkedEvent;

        } catch (final SQLException e) {
            throw new DataAccessException("Failed to add PublishedEvent to batch", e);
        }
    }

    public void insertBatch() {

        try {
            preparedStatement.executeBatch();
        } catch (final SQLException e) {
            throw new DataAccessException("Failed to insert batch of PublishedEvents", e);
        }
    }

    @Override
    public void close() {
       closer.closeQuietly(preparedStatement);
       closer.closeQuietly(connection);
    }
}
