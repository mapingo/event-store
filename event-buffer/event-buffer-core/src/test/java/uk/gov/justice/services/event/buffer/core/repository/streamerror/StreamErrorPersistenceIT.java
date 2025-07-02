package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

@ExtendWith(MockitoExtension.class)
public class StreamErrorPersistenceIT {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreDataSourceProvider;

    @Spy
    private StreamErrorHashRowMapper streamErrorHashRowMapper;
    @InjectMocks
    private StreamErrorHashPersistence streamErrorHashPersistence;
    @Spy
    private StreamErrorDetailsRowMapper streamErrorDetailsRowMapper;
    @InjectMocks
    private StreamErrorDetailsPersistence streamErrorDetailsPersistence;
    @Spy
    private StreamErrorPersistence streamErrorPersistence;

    private final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource("framework");
    private final DatabaseCleaner databaseCleaner = new DatabaseCleaner();

    @BeforeEach
    public void cleanTables() {
        setField(streamErrorPersistence, "streamErrorHashPersistence", streamErrorHashPersistence);
        setField(streamErrorPersistence, "streamErrorDetailsPersistence", streamErrorDetailsPersistence);
        databaseCleaner.cleanViewStoreTables("framework", "stream_error_hash", "stream_error");
    }

    @Test
    public void shouldSaveAndRemoveErrorsCorrectly() throws Exception {

        final UUID streamId = randomUUID();
        final String hash = "this-is-a-hash";
        final String source = "this-is-the-source";
        final String componentName_1 = "EVENT_LISTENER";
        final String componentName_2 = "EVENT_INDEXER";

        // add 2 errors which both have the same hash
        final StreamErrorDetails streamErrorDetails_1 = aStreamErrorDetails(streamId, hash, componentName_1, source);
        final StreamErrorDetails streamErrorDetails_2 = aStreamErrorDetails(streamId, hash, componentName_2, source);
        final StreamErrorHash streamErrorHash = aStreamErrorHash(hash);


        try (final Connection connection = viewStoreDataSource.getConnection()) {

            streamErrorPersistence.save(new StreamError(streamErrorDetails_1, streamErrorHash), connection);
            streamErrorPersistence.save(new StreamError(streamErrorDetails_2, streamErrorHash), connection);

            // check everything was saved
            final Optional<StreamErrorHash> optionalStreamErrorHash = streamErrorHashPersistence.findByHash(hash, connection);
            assertThat(optionalStreamErrorHash, is(of(streamErrorHash)));
            final List<StreamErrorDetails> streamErrorDetails = streamErrorDetailsPersistence.findAll(connection);

            assertThat(streamErrorDetails.size(), is(2));
            assertThat(streamErrorDetails.get(0), is(streamErrorDetails_1));
            assertThat(streamErrorDetails.get(1), is(streamErrorDetails_2));
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {

            // remove one of the errors
            streamErrorPersistence.removeErrorForStream(streamId, source, componentName_2, connection);

            final List<StreamErrorDetails> streamErrorDetails = streamErrorDetailsPersistence.findAll(connection);

            // now only one error remaining
            assertThat(streamErrorDetails.size(), is(1));
            assertThat(streamErrorDetails.get(0), is(streamErrorDetails_1));

            // but the hash wasn't deleted
            final Optional<StreamErrorHash> optionalStreamErrorHash = streamErrorHashPersistence.findByHash(hash, connection);
            assertThat(optionalStreamErrorHash, is(of(streamErrorHash)));
        }

        try (final Connection connection = viewStoreDataSource.getConnection()) {

            // remove the final error
            streamErrorPersistence.removeErrorForStream(streamId, source, componentName_1, connection);

            // now no errors remaining
            final List<StreamErrorDetails> streamErrorDetails = streamErrorDetailsPersistence.findAll(connection);
            assertThat(streamErrorDetails.isEmpty(), is(true));
            // and the hash has also been deleted
            final Optional<StreamErrorHash> optionalStreamErrorHash = streamErrorHashPersistence.findByHash(hash, connection);
            assertThat(optionalStreamErrorHash, is(empty()));
        }
    }

    private StreamErrorDetails aStreamErrorDetails(
            final UUID streamId,
            final String hash,
            final String componentName,
            final String source) {

        return new StreamErrorDetails(
                randomUUID(),
                hash,
                "some-exception-message",
                empty(),
                "event-name",
                randomUUID(),
                streamId,
                234L,
                new UtcClock().now(),
                "stack-trace",
                componentName,
                source
        );
    }

    private StreamErrorHash aStreamErrorHash(final String hash) {
        return new StreamErrorHash(
                hash,
                "exception-class-name",
                empty(),
                "java-class-name",
                "java-method",
                23
        );
    }
}