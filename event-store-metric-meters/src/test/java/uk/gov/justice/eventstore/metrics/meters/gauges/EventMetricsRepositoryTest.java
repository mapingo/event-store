package uk.gov.justice.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.metrics.micrometer.meters.MetricsProviderException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventMetricsRepositoryTest {

    private static final String COUNT_STREAMS_SQL = """
                SELECT COUNT(*) AS number_of_streams
                FROM (
                    SELECT DISTINCT stream_id FROM stream_status
                ) AS temp;
            """;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private EventMetricsRepository eventMetricsRepository;

    @Test
    public void shouldCountTheNumberOfStreamsInStreamStatusTable() throws Exception {

        final int numberOfStreams = 23;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(COUNT_STREAMS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(numberOfStreams);

        assertThat(eventMetricsRepository.countStreams(), is(numberOfStreams));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldReturnZeroStreamsIfNoResultsReturned() throws Exception {

        final int zeroStreams = 0;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(COUNT_STREAMS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(eventMetricsRepository.countStreams(), is(zeroStreams));

        verify(resultSet, never()).getInt(1);

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }

    @Test
    public void shouldThrowMetricsProviderExceptionIfQueryingTableFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(COUNT_STREAMS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(sqlException);

        final MetricsProviderException metricsProviderException = assertThrows(
                MetricsProviderException.class,
                () -> eventMetricsRepository.countStreams());

        assertThat(metricsProviderException.getCause(), is(sqlException));
        assertThat(metricsProviderException.getMessage(), is("Failed to count total number of streams in stream_status table"));

        verify(connection).close();
        verify(preparedStatement).close();
        verify(resultSet).close();
    }
}