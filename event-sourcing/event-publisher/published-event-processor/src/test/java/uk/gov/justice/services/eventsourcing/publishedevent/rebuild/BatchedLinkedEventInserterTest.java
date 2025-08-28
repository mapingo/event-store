package uk.gov.justice.services.eventsourcing.publishedevent.rebuild;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.PublishedEventStatements.INSERT_INTO_PUBLISHED_EVENT_SQL;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.util.io.Closer;
import uk.gov.justice.services.jdbc.persistence.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BatchedLinkedEventInserterTest {

    @Mock
    private Closer closer;

    @InjectMocks
    private BatchedPublishedEventInserter batchedPublishedEventInserter;

    @SuppressWarnings("ConstantConditions")
    @Test
    public void shouldAddToBatchForInsert() throws Exception {

        final LinkedEvent linkedEvent = aPublishedEvent();

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)).thenReturn(preparedStatement);

        batchedPublishedEventInserter.prepareForInserts(eventStoreDataSource);
        batchedPublishedEventInserter.addToBatch(linkedEvent);

        final InOrder inOrder = inOrder(preparedStatement);

        inOrder.verify(preparedStatement).setObject(1, linkedEvent.getId());
        inOrder.verify(preparedStatement).setObject(2, linkedEvent.getStreamId());
        inOrder.verify(preparedStatement).setLong(3, linkedEvent.getPositionInStream());
        inOrder.verify(preparedStatement).setString(4, linkedEvent.getName());
        inOrder.verify(preparedStatement).setString(5, linkedEvent.getPayload());
        inOrder.verify(preparedStatement).setString(6, linkedEvent.getMetadata());
        inOrder.verify(preparedStatement).setObject(7, toSqlTimestamp(linkedEvent.getCreatedAt()));
        inOrder.verify(preparedStatement).setLong(8, linkedEvent.getEventNumber().orElse(null));
        inOrder.verify(preparedStatement).setLong(9, linkedEvent.getPreviousEventNumber());

        inOrder.verify(preparedStatement).addBatch();

        verify(preparedStatement, never()).executeBatch();

        batchedPublishedEventInserter.insertBatch();

        verify(preparedStatement).executeBatch();
    }

    @Test
    public void shouldCloseStatementAndConnectionOnClose() throws Exception {

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)).thenReturn(preparedStatement);

        batchedPublishedEventInserter.prepareForInserts(eventStoreDataSource);

        batchedPublishedEventInserter.close();

        final InOrder inOrder = inOrder(closer);

        inOrder.verify(closer).closeQuietly(preparedStatement);
        inOrder.verify(closer).closeQuietly(connection);
    }

    @Test
    public void shouldThrowExceptionIfPreparingForInsertsFails() throws Exception {

        final SQLException sqlException = new SQLException("Oops");

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)).thenThrow(sqlException);

        try {
            batchedPublishedEventInserter.prepareForInserts(eventStoreDataSource);
            fail();
        } catch (final DataAccessException expected) {
            assertThat(expected.getCause(), is(sqlException));
            assertThat(expected.getMessage(), is("Failed to prepare statement for batch insert of PublishedEvents"));
        }
    }

    @Test
    public void shouldThrowExceptionIfAddingToBatchFails() throws Exception {

        final SQLException sqlException = new SQLException("Oops");

        final LinkedEvent linkedEvent = aPublishedEvent();

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)).thenReturn(preparedStatement);

        batchedPublishedEventInserter.prepareForInserts(eventStoreDataSource);

        doThrow(sqlException).when(preparedStatement).addBatch();

        try {
            batchedPublishedEventInserter.addToBatch(linkedEvent);
            fail();
        } catch (final DataAccessException expected) {
            assertThat(expected.getCause(), is(sqlException));
            assertThat(expected.getMessage(), is("Failed to add PublishedEvent to batch"));
        }
    }

    @Test
    public void shouldThrowExceptionIfExecutingBatchFails() throws Exception {

        final SQLException sqlException = new SQLException("Oops");

        final DataSource eventStoreDataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)).thenReturn(preparedStatement);

        batchedPublishedEventInserter.prepareForInserts(eventStoreDataSource);

        doThrow(sqlException).when(preparedStatement).executeBatch();

        try {
            batchedPublishedEventInserter.insertBatch();
            fail();
        } catch (final DataAccessException expected) {
            assertThat(expected.getCause(), is(sqlException));
            assertThat(expected.getMessage(), is("Failed to insert batch of PublishedEvents"));
        }
    }

    private LinkedEvent aPublishedEvent() {
        return new LinkedEvent(
                randomUUID(),
                randomUUID(),
                23L,
                "event-name",
                "metadata",
                "payload",
                new UtcClock().now(),
                234L,
                233L
        );
    }
}
