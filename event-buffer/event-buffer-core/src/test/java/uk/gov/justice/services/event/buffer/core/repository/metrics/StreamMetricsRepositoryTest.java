package uk.gov.justice.services.event.buffer.core.repository.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamMetricsRepositoryTest {

    private static final String STREAM_METRICS_SQL = """
            SELECT * FROM (
            SELECT
                    source,
                    component,
                    COUNT(*)                                              AS stream_count,
                    COUNT(*) FILTER (WHERE is_up_to_date)                 AS up_to_date_streams_count,
                    COUNT(*) FILTER (WHERE NOT is_up_to_date)             AS out_of_date_streams_count,
                    COUNT(*) FILTER (WHERE stream_error_id IS NOT NULL)   AS blocked_streams_count,
                    COUNT(*) FILTER (WHERE stream_error_id IS NULL)       AS unblocked_streams_count
                FROM stream_status
                GROUP BY source, component
            ) AS metrics
            WHERE metrics.source = ? AND metrics.component = ?;
            """;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;
    
    @InjectMocks
    private StreamMetricsRepository streamMetricsRepository;

    @Test
    public void shouldGetMetricsFromTheStreamStatusTable() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final int streamCount = 23;
        final int upToDateStreamCount = 345;
        final int outOfDateStreamCount = 9782;
        final int blockedStreamCount = 252;
        final int unblockedStreamCount = 727;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(STREAM_METRICS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        when(resultSet.getInt("stream_count")).thenReturn(streamCount);
        when(resultSet.getInt("up_to_date_streams_count")).thenReturn(upToDateStreamCount);
        when(resultSet.getInt("out_of_date_streams_count")).thenReturn(outOfDateStreamCount);
        when(resultSet.getInt("blocked_streams_count")).thenReturn(blockedStreamCount);
        when(resultSet.getInt("unblocked_streams_count")).thenReturn(unblockedStreamCount);

        final Optional<StreamMetrics> streamMetrics = streamMetricsRepository.getStreamMetrics(source, component);

        if (streamMetrics.isPresent()) {
            assertThat(streamMetrics.get().source(), is(source));
            assertThat(streamMetrics.get().component(), is(component));
            assertThat(streamMetrics.get().streamCount(), is(streamCount));
            assertThat(streamMetrics.get().upToDateStreamCount(), is(upToDateStreamCount));
            assertThat(streamMetrics.get().outOfDateStreamCount(), is(outOfDateStreamCount));
            assertThat(streamMetrics.get().blockedStreamCount(), is(blockedStreamCount));
            assertThat(streamMetrics.get().unblockedStreamCount(), is(unblockedStreamCount));
        } else {
            fail();
        }

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);
        inOrder.verify(preparedStatement).setString(1, source);
        inOrder.verify(preparedStatement).setString(2, component);
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowMetricsJdbcExceptionIfDatabaseAccessFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");
        final String source = "some-source";
        final String component = "some-component";
        final int streamCount = 23;
        final int upToDateStreamCount = 345;
        final int outOfDateStreamCount = 9782;
        final int blockedStreamCount = 252;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(STREAM_METRICS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        when(resultSet.getInt("stream_count")).thenReturn(streamCount);
        when(resultSet.getInt("up_to_date_streams_count")).thenReturn(upToDateStreamCount);
        when(resultSet.getInt("out_of_date_streams_count")).thenReturn(outOfDateStreamCount);
        when(resultSet.getInt("blocked_streams_count")).thenReturn(blockedStreamCount);
        when(resultSet.getInt("unblocked_streams_count")).thenThrow(sqlException);

        final MetricsJdbcException metricsJdbcException = assertThrows(
                MetricsJdbcException.class,
                () -> streamMetricsRepository.getStreamMetrics(source, component));

        assertThat(metricsJdbcException.getCause(), is(sqlException));
        assertThat(metricsJdbcException.getMessage(), is("Failed to get metrics from stream_status table"));

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }
}