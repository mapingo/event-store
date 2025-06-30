package uk.gov.justice.services.event.sourcing.subscription.error.servlet;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorFinderTest {

    @Mock
    private StreamErrorRepository streamErrorRepository;
    
    @InjectMocks
    private StreamErrorFinder streamErrorFinder;

    @Test
    public void shouldGetTheListOfStreamErrorsByStreamId() throws Exception {

        final UUID streamId = randomUUID();

        final StreamError streamError_1 = mock(StreamError.class);
        final StreamError streamError_2 = mock(StreamError.class);

        when(streamErrorRepository.findAllByStreamId(streamId)).thenReturn(List.of(streamError_1, streamError_2));

        final List<StreamError> streamErrors = streamErrorFinder.findByStreamId(streamId);
        assertThat(streamErrors.size(), is(2));
        assertThat(streamErrors.get(0), is(sameInstance(streamError_1)));
        assertThat(streamErrors.get(1), is(sameInstance(streamError_2)));
    }

    @Test
    public void shouldFindStreamErrorByIdAndReturnAsList() throws Exception {

        final UUID errorId = randomUUID();
        final StreamError streamError = mock(StreamError.class);

        when(streamErrorRepository.findByErrorId(errorId)).thenReturn(of(streamError));

        final List<StreamError> streamErrors = streamErrorFinder.findByErrorId(errorId);

        assertThat(streamErrors.size(), is(1));
        assertThat(streamErrors.get(0), is(sameInstance(streamError)));
    }

    @Test
    public void shouldReturnEmptyListIfNoStreamErrorsFoundForId() throws Exception {

        final UUID errorId = randomUUID();
        
        when(streamErrorRepository.findByErrorId(errorId)).thenReturn(empty());

        final List<StreamError> streamErrors = streamErrorFinder.findByErrorId(errorId);

        assertThat(streamErrors.size(), is(0));
    }
}