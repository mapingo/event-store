package uk.gov.justice.services.event.buffer.core.repository.subscription;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Optional;
import java.util.UUID;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

public class StreamUpdateContextTest {

    @Test
    public void shouldReturnStreamCurrentlyErroredIfStreamErrorIdIsNotEmpty() throws Exception {

        final UUID streamErrorId = randomUUID();
        final StreamUpdateContext erroredStreamUpdateContext = new StreamUpdateContext(1, 2, 3, of(streamErrorId));
        final StreamUpdateContext fixedStreamUpdateContext = new StreamUpdateContext(1, 2, 3, empty());

        assertThat(erroredStreamUpdateContext.streamCurrentlyErrored(), is(true));
        assertThat(fixedStreamUpdateContext.streamCurrentlyErrored(), is(false));

    }
}