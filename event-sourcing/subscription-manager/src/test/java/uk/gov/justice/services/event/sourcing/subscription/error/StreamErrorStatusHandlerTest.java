package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.UUID.fromString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHandlingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import javax.transaction.UserTransaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamErrorStatusHandlerTest {

    @Mock
    private ExceptionDetailsRetriever exceptionDetailsRetriever;

    @Mock
    private StreamErrorConverter streamErrorConverter;

    @Mock
    private StreamErrorRepository streamErrorRepository;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Test
    public void shouldCreateEventErrorFromExceptionAndJsonEnvelopeAndSave() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException();
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError streamError = mock(StreamError.class);
        final String componentName = "SOME_COMPONENT";

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, componentName)).thenReturn(streamError);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, componentName);

        final InOrder inOrder = inOrder(transactionHandler, streamErrorRepository);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markStreamAsErrored(streamError);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
    }

    @Test
    public void shouldRollBackAndLogIfUpdatingErrorTableFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException();
        final StreamErrorHandlingException streamErrorHandlingException = new StreamErrorHandlingException("dsfkjh");
        final String componentName = "SOME_COMPONENT";
        final UUID streamId = fromString("788cc64e-d31e-46fb-975f-b19042bb0a13");

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError streamError = mock(StreamError.class);
        final StreamErrorDetails streamErrorDetails = mock(StreamErrorDetails.class);

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, componentName)).thenReturn(streamError);
        when(streamError.streamErrorDetails()).thenReturn(streamErrorDetails);
        when(streamErrorDetails.streamId()).thenReturn(streamId);
        doThrow(streamErrorHandlingException).when(streamErrorRepository).markStreamAsErrored(streamError);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, componentName);

        final InOrder inOrder = inOrder(transactionHandler, streamErrorRepository, logger);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markStreamAsErrored(streamError);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(logger).error("Failed to mark stream as errored: streamId '788cc64e-d31e-46fb-975f-b19042bb0a13'", streamErrorHandlingException);

        verify(transactionHandler, never()).commit(userTransaction);
    }

}