package uk.gov.justice.services.event.sourcing.subscription.error;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;

public class StreamErrorStatusHandler {

    @Inject
    private ExceptionDetailsRetriever exceptionDetailsRetriever;

    @Inject
    private StreamErrorConverter streamErrorConverter;

    @Inject
    private StreamErrorRepository streamErrorRepository;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private Logger logger;

    public void onStreamProcessingFailure(final JsonEnvelope jsonEnvelope, final Throwable exception, final String componentName) {

        final ExceptionDetails exceptionDetails = exceptionDetailsRetriever.getExceptionDetailsFrom(exception);
        final StreamError streamError = streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, componentName);
        try {
            transactionHandler.begin(userTransaction);
            streamErrorRepository.markStreamAsErrored(streamError);
            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            final String formatted = "Failed to mark stream as errored: streamId '%s'".formatted(streamError.streamErrorDetails().streamId());

            System.out.println(formatted);

            logger.error(formatted, e);
        }
    }
}
