package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewEventBufferRepositoryTest {

    private static final String INSERT_SQL = """
            INSERT INTO stream_buffer (
                stream_id,
                position,
                event,
                source,
                component,
                buffered_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private static final String FIND_BY_POSITION_SQL = """
            SELECT event, buffered_at FROM stream_buffer
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            AND position = ?
            """;

    private static final String REMOVE_FROM_BUFFER_SQL = """
            DELETE FROM stream_buffer
            WHERE stream_id = ?
            AND source = ?
            AND component = ?
            AND position = ?
            """;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;
    
    @InjectMocks
    private NewEventBufferRepository newEventBufferRepository;

    @Test
    public void shouldInsertEventBufferEvent() throws Exception {

        final UUID streamId = randomUUID();
        final long position = 98734;
        final String eventJson = "some-event-as-json";
        final String source = "some-source";
        final String componentName = "some-component-name";
        final ZonedDateTime bufferedAt = new UtcClock().now();
        final int rowsAffected = 1;

        final EventBufferEvent eventBufferEvent = new EventBufferEvent(
                streamId,
                position,
                eventJson,
                source,
                componentName,
                bufferedAt
        );

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(rowsAffected);

        assertThat(newEventBufferRepository.insert(eventBufferEvent), is(rowsAffected));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setLong(2, position);
        inOrder.verify(preparedStatement).setString(3, eventJson);
        inOrder.verify(preparedStatement).setString(4, source);
        inOrder.verify(preparedStatement).setString(5, componentName);
        inOrder.verify(preparedStatement).setTimestamp(6, toSqlTimestamp(bufferedAt));
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventBufferPersistenceExceptionIfInsertIntoEventBufferFails() throws Exception {

        final UUID streamId = fromString("301d713e-b06f-4926-8084-6cc6bdbe832b");
        final long position = 98734;
        final String eventJson = "some-event-as-json";
        final String source = "some-source";
        final String componentName = "some-component-name";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final SQLException sqlException = new SQLException("Ooops");

        final EventBufferEvent eventBufferEvent = new EventBufferEvent(
                streamId,
                position,
                eventJson,
                source,
                componentName,
                bufferedAt
        );

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_SQL)).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final EventBufferPersistenceException eventBufferPersistenceException = assertThrows(
                EventBufferPersistenceException.class,
                () -> newEventBufferRepository.insert(eventBufferEvent));

        assertThat(eventBufferPersistenceException.getCause(), is(sqlException));
        assertThat(eventBufferPersistenceException.getMessage(), is("Failed to insert event into event-buffer table. streamId '301d713e-b06f-4926-8084-6cc6bdbe832b'. source 'some-source', component 'some-component-name'"));

        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldFindByPositionAndStream() throws Exception {

        final UUID streamId = randomUUID();
        final long position = 98734;
        final String eventJson = "some-event-as-json";
        final String source = "some-source";
        final String componentName = "some-component-name";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_POSITION_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        when(resultSet.getString("event")).thenReturn(eventJson);
        when(resultSet.getTimestamp("buffered_at")).thenReturn(toSqlTimestamp(bufferedAt));

        final Optional<EventBufferEvent> nextEvent = newEventBufferRepository.findByPositionAndStream(
                streamId,
                position,
                source,
                componentName);

        assertThat(nextEvent.isPresent(), is(true));
        assertThat(nextEvent.get().getPosition(), is(position));
        assertThat(nextEvent.get().getStreamId(), is(streamId));
        assertThat(nextEvent.get().getSource(), is(source));
        assertThat(nextEvent.get().getBufferedAt(), is(bufferedAt));
        assertThat(nextEvent.get().getComponent(), is(componentName));

        verify(preparedStatement).setObject(1, streamId);
        verify(preparedStatement).setString(2, source);
        verify(preparedStatement).setString(3, componentName);
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldReturnEmptyIfNoNextEventFound() throws Exception {

        final UUID streamId = randomUUID();
        final long position = 98734;
        final String source = "some-source";
        final String componentName = "some-component-name";

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_POSITION_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final Optional<EventBufferEvent> nextEvent = newEventBufferRepository.findByPositionAndStream(
                streamId,
                position,
                source,
                componentName);

        assertThat(nextEvent.isEmpty(), is(true));

        verify(preparedStatement).setObject(1, streamId);
        verify(preparedStatement).setString(2, source);
        verify(preparedStatement).setString(3, componentName);
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldThrowEventBufferPersistenceExceptionIfFindNextFromEventBufferFails() throws Exception {

        final UUID streamId = fromString("02a88780-df87-42aa-bd8c-5f4bb03ebd0e");
        final String source = "some-source";
        final String componentName = "some-component-name";
        final long position = 98734;

        final SQLException sqlException = new SQLException("Bobbins");

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_POSITION_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(sqlException);

        final EventBufferPersistenceException eventBufferPersistenceException = assertThrows(
                EventBufferPersistenceException.class,
                () -> newEventBufferRepository.findByPositionAndStream(
                        streamId,
                        position,
                        source,
                        componentName));

        assertThat(eventBufferPersistenceException.getCause(), is(sqlException));
        assertThat(eventBufferPersistenceException.getMessage(), is("Failed to get next event from event buffer: streamId '02a88780-df87-42aa-bd8c-5f4bb03ebd0e', source 'some-source', componentName 'some-component-name'"));

        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    public void shouldRemoveEventFromEventBuffer() throws Exception {

        final UUID streamId = fromString("301d713e-b06f-4926-8084-6cc6bdbe832b");
        final long position = 98734;
        final String source = "some-source";
        final String componentName = "some-component-name";

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(REMOVE_FROM_BUFFER_SQL)).thenReturn(preparedStatement);

        newEventBufferRepository.remove(streamId, source, componentName, position);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setObject(1, streamId);
        inOrder.verify(preparedStatement).setString(2, source);
        inOrder.verify(preparedStatement).setString(3, componentName);
        inOrder.verify(preparedStatement).setLong(4, position);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventBufferPersistenceExceptionIfRemovingEventFromEventBufferFails() throws Exception {

        final UUID streamId = fromString("301d713e-b06f-4926-8084-6cc6bdbe832b");
        final long position = 98734;
        final String source = "some-source";
        final String componentName = "some-component-name";

        final SQLException sqlException = new SQLException("Ooops");

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(REMOVE_FROM_BUFFER_SQL)).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final EventBufferPersistenceException eventBufferPersistenceException = assertThrows(
                EventBufferPersistenceException.class,
                () -> newEventBufferRepository.remove(streamId, source, componentName, position));

        assertThat(eventBufferPersistenceException.getCause(), is(sqlException));
        assertThat(eventBufferPersistenceException.getMessage(), is("Failed to remove event from event-buffer table. streamId '301d713e-b06f-4926-8084-6cc6bdbe832b'. source 'some-source', component 'some-component-name', position '98734'"));

        verify(preparedStatement).close();
        verify(connection).close();
    }
}