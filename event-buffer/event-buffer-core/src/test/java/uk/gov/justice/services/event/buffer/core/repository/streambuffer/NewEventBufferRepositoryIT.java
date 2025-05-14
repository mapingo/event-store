package uk.gov.justice.services.event.buffer.core.repository.streambuffer;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.TestJdbcDataSourceProvider;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewEventBufferRepositoryIT {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private NewEventBufferRepository newEventBufferRepository;

    @BeforeEach
    public void cleanTables() {
       new DatabaseCleaner().cleanStreamBufferTable("framework");
    }

    @Test
    public void shouldInsertFindFirstAndRemoveEventFromEventInsertTable() throws Exception {

        final ZonedDateTime now = new UtcClock().now();
        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource("framework");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(clock.now()).thenReturn(now);

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final EventBufferEvent eventBufferEvent_1 = new EventBufferEvent(
                streamId,
                1L,
                "some-event-json-1",
                source,
                component,
                clock.now()
        );
        final EventBufferEvent eventBufferEvent_2 = new EventBufferEvent(
                streamId,
                2L,
                "some-event-json-2",
                source,
                component,
                clock.now()
        );
        final EventBufferEvent eventBufferEvent_3 = new EventBufferEvent(
                streamId,
                3L,
                "some-event-json-3",
                source,
                component,
                clock.now()
        );

        newEventBufferRepository.insert(eventBufferEvent_1);
        newEventBufferRepository.insert(eventBufferEvent_3);
        newEventBufferRepository.insert(eventBufferEvent_2);

        assertThat(newEventBufferRepository.findByPositionAndStream(streamId, eventBufferEvent_1.getPosition(), source, component), is(of(eventBufferEvent_1)));
        newEventBufferRepository.remove(
                eventBufferEvent_1.getStreamId(),
                eventBufferEvent_1.getSource(),
                eventBufferEvent_1.getComponent(),
                eventBufferEvent_1.getPosition());

        assertThat(newEventBufferRepository.findByPositionAndStream(streamId, eventBufferEvent_2.getPosition(), source, component), is(of(eventBufferEvent_2)));
        newEventBufferRepository.remove(
                eventBufferEvent_2.getStreamId(),
                eventBufferEvent_2.getSource(),
                eventBufferEvent_2.getComponent(),
                eventBufferEvent_2.getPosition());

        assertThat(newEventBufferRepository.findByPositionAndStream(streamId, eventBufferEvent_3.getPosition(), source, component), is(of(eventBufferEvent_3)));
        newEventBufferRepository.remove(
                eventBufferEvent_3.getStreamId(),
                eventBufferEvent_3.getSource(),
                eventBufferEvent_3.getComponent(),
                eventBufferEvent_3.getPosition());

        assertThat(newEventBufferRepository.findByPositionAndStream(streamId, 4, source, component), is(empty()));
    }

    @Test
    public void shouldInsertIdempotently() throws Exception {

        final ZonedDateTime now = new UtcClock().now();
        final DataSource viewStoreDataSource = new TestJdbcDataSourceProvider().getViewStoreDataSource("framework");

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
        when(clock.now()).thenReturn(now);

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final EventBufferEvent eventBufferEvent = new EventBufferEvent(
                streamId,
                23,
                "some-more-event-json",
                source,
                component,
                clock.now()
        );

        assertThat(newEventBufferRepository.insert(eventBufferEvent), is(1));
        assertThat(newEventBufferRepository.insert(eventBufferEvent), is(0));
        assertThat(newEventBufferRepository.insert(eventBufferEvent), is(0));
        assertThat(newEventBufferRepository.insert(eventBufferEvent), is(0));
        assertThat(newEventBufferRepository.insert(eventBufferEvent), is(0));

        assertThat(newEventBufferRepository.findByPositionAndStream(streamId, eventBufferEvent.getPosition(), source, component), is(of(eventBufferEvent)));
    }
}