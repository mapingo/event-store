package uk.gov.justice.services.healthcheck.healthchecks.artemis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.justice.services.messaging.jms.DestinationProvider;
import uk.gov.justice.services.messaging.jms.exception.JmsEnvelopeSenderException;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JmsDestinationsVerifierTest {

    @Mock
    private DestinationNamesProvider destinationNamesProvider;

    @Mock
    private DestinationProvider destinationProvider;

    @InjectMocks
    private JmsDestinationsVerifier jmsDestinationsVerifier;

    @Captor
    ArgumentCaptor<String> destinationNameCaptor;

    @Test
    public void shouldVerifyThatAllDestinationsExists() throws Exception {
        var session = mock(Session.class);
        var destination = mock(Destination.class);
        var consumer = mock(MessageConsumer.class);
        given(destinationNamesProvider.getDestinationNames()).willReturn(List.of("queue-1", "queue-2"));
        given(destinationProvider.getDestination(any())).willReturn(destination);
        given(session.createConsumer(destination)).willReturn(consumer);

        jmsDestinationsVerifier.verify(session);

        verify(destinationProvider, times(2)).getDestination(destinationNameCaptor.capture());
        assertThat(destinationNameCaptor.getAllValues(), hasItems("queue-1", "queue-2"));
        verify(session, times(2)).createConsumer(destination);
        verify(consumer, times(2)).close();
    }

    @Test(expected = DestinationNotFoundException.class)
    public void shouldThrowExceptionWhenInvalidDestinationException() throws Exception {
        var session = mock(Session.class);
        given(destinationNamesProvider.getDestinationNames()).willReturn(List.of("queue-1"));
        doThrow(new JmsEnvelopeSenderException("Ex message", new RuntimeException())).when(destinationProvider).getDestination("queue-1");

        jmsDestinationsVerifier.verify(session);
    }
}