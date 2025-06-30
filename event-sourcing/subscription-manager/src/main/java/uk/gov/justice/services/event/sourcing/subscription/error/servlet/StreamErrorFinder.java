package uk.gov.justice.services.event.sourcing.subscription.error.servlet;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

public class StreamErrorFinder {

    @Inject
    private StreamErrorRepository streamErrorRepository;

    public List<StreamError> findByStreamId(final UUID streamId) {
        return streamErrorRepository.findAllByStreamId(streamId);
    }

    public List<StreamError> findByErrorId(final UUID errorId) {
        return streamErrorRepository.findByErrorId(errorId)
                .stream()
                .toList();
    }
}
