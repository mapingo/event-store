package uk.gov.justice.services.event.buffer.core.repository.metrics;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

public class StreamMetricsRepository {

    private static final String STREAM_METRICS_SQL = """
            SELECT
                    source,
                    component,
                    COUNT(*)                                              AS stream_count,
                    COUNT(*) FILTER (WHERE is_up_to_date)                 AS up_to_date_streams_count,
                    COUNT(*) FILTER (WHERE NOT is_up_to_date)             AS out_of_date_streams_count,
                    COUNT(*) FILTER (WHERE stream_error_id IS NOT NULL)   AS blocked_streams_count,
                    COUNT(*) FILTER (WHERE stream_error_id IS NULL)       AS unblocked_streams_count
                FROM stream_status
                GROUP BY source, component;
            """;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public List<StreamMetrics> getStreamMetrics() {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(STREAM_METRICS_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            final ArrayList<StreamMetrics> streamMetrics = new ArrayList<>();

            while (resultSet.next()) {
                final String source = resultSet.getString("source");
                final String component = resultSet.getString("component");
                final int streamCount = resultSet.getInt("stream_count");
                final int upToDateStreamsCount = resultSet.getInt("up_to_date_streams_count");
                final int outOfDateStreamsCount = resultSet.getInt("out_of_date_streams_count");
                final int blockedStreamsCount = resultSet.getInt("blocked_streams_count");
                final int unblockedStreamsCount = resultSet.getInt("unblocked_streams_count");

                streamMetrics.add(new StreamMetrics(
                        source,
                        component,
                        streamCount,
                        upToDateStreamsCount,
                        outOfDateStreamsCount,
                        blockedStreamsCount,
                        unblockedStreamsCount
                ));
            }

            return streamMetrics;

        } catch (final SQLException e) {
            throw new MetricsJdbcException("Failed to get metrics from stream_status table", e);
        }
    }
}
