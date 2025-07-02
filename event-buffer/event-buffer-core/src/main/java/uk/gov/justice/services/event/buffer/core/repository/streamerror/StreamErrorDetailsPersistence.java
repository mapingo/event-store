package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

@SuppressWarnings("java:S1192")
public class StreamErrorDetailsPersistence {

    private static final String INSERT_EXCEPTION_SQL = """
            INSERT INTO stream_error (
                id,
                hash,
                exception_message,
                cause_message,
                event_name,
                event_id,
                stream_id,
                position_in_stream,
                date_created,
                full_stack_trace,
                component,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, component, source) DO NOTHING
            """;

    private static final String SELECT_CLAUSE = """
            SELECT
                id,
                hash,
                exception_message,
                cause_message,
                event_name,
                event_id,
                stream_id,
                position_in_stream,
                date_created,
                full_stack_trace,
                component,
                source
            """;
    private static final String FIND_BY_ID_SQL = """
            %s
            FROM stream_error
            WHERE id = ?
            """.formatted(SELECT_CLAUSE);

    private static final String FIND_BY_STREAM_ID_SQL = """
            %s
            FROM stream_error
            WHERE stream_id = ?
            """.formatted(SELECT_CLAUSE);

    private static final String FIND_ALL_SQL = """
            %s
            FROM stream_error
            """.formatted(SELECT_CLAUSE);

    private static final String DELETE_SQL = "DELETE FROM stream_error WHERE stream_id = ? AND source = ? AND component = ?";

    @Inject
    private StreamErrorDetailsRowMapper streamErrorDetailsRowMapper;

    public int insert(final StreamErrorDetails streamErrorDetails, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_EXCEPTION_SQL)) {
            preparedStatement.setObject(1, streamErrorDetails.id());
            preparedStatement.setString(2, streamErrorDetails.hash());
            preparedStatement.setString(3, streamErrorDetails.exceptionMessage());
            preparedStatement.setString(4, streamErrorDetails.causeMessage().orElse(null));
            preparedStatement.setString(5, streamErrorDetails.eventName());
            preparedStatement.setObject(6, streamErrorDetails.eventId());
            preparedStatement.setObject(7, streamErrorDetails.streamId());
            preparedStatement.setLong(8, streamErrorDetails.positionInStream());
            preparedStatement.setTimestamp(9, toSqlTimestamp(streamErrorDetails.dateCreated()));
            preparedStatement.setString(10, streamErrorDetails.fullStackTrace());
            preparedStatement.setString(11, streamErrorDetails.componentName());
            preparedStatement.setString(12, streamErrorDetails.source());

            return preparedStatement.executeUpdate();
        }
    }

    public Optional<StreamErrorDetails> findById(final UUID id, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_ID_SQL)) {
            preparedStatement.setObject(1, id);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final StreamErrorDetails streamErrorDetails = streamErrorDetailsRowMapper.mapRow(resultSet);
                    return of(streamErrorDetails);
                }

                return empty();
            }
        }
    }

    public List<StreamErrorDetails> findByStreamId(final UUID streamId, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_STREAM_ID_SQL)) {
            preparedStatement.setObject(1, streamId);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                final ArrayList<StreamErrorDetails> streamErrorDetailsList = new ArrayList<>();
                while (resultSet.next()) {
                    final StreamErrorDetails streamErrorDetails = streamErrorDetailsRowMapper.mapRow(resultSet);
                    streamErrorDetailsList.add(streamErrorDetails);
                }

                return streamErrorDetailsList;
            }
        }
    }

    public List<StreamErrorDetails> findAll(final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL)) {
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ArrayList<StreamErrorDetails> streamErrorDetailsList = new ArrayList<>();
                while (resultSet.next()) {
                    final StreamErrorDetails streamErrorDetails = streamErrorDetailsRowMapper.mapRow(resultSet);
                    streamErrorDetailsList.add(streamErrorDetails);
                }

                return streamErrorDetailsList;
            }
        }
    }

    public void deleteBy(final UUID streamId, final String source, final String componentName, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(DELETE_SQL)) {
            preparedStatement.setObject(1, streamId);
            preparedStatement.setString(2, source);
            preparedStatement.setString(3, componentName);
            preparedStatement.executeUpdate();
        }
    }
}
