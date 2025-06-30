package uk.gov.justice.services.event.sourcing.subscription.error.servlet;

import static java.util.UUID.randomUUID;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;

import java.io.PrintWriter;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorServletTest {

    @Mock
    private StreamErrorFinder streamErrorFinder;

    @Mock
    private StreamErrorsToJsonConverter streamErrorsToJsonConverter;

    @InjectMocks
    private StreamErrorServlet streamErrorServlet;

    @Test
    public void shouldGetErrorJsonByStreamId() throws Exception {

        final UUID streamId = randomUUID();
        final String errorJson = "some-json";

        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
        final PrintWriter out = mock(PrintWriter.class);
        final List<StreamError> streamErrors = List.of(mock(StreamError.class));

        when(httpServletRequest.getParameter("streamId")).thenReturn(streamId.toString());
        when(streamErrorFinder.findByStreamId(streamId)).thenReturn(streamErrors);
        when(streamErrorsToJsonConverter.toJson(streamErrors)).thenReturn(errorJson);
        when(httpServletResponse.getWriter()).thenReturn(out);

        streamErrorServlet.doGet(httpServletRequest, httpServletResponse);

        final InOrder inOrder = inOrder(httpServletResponse, out);

        inOrder.verify(httpServletResponse).setContentType("application/json; charset=UTF-8");
        inOrder.verify(httpServletResponse).setCharacterEncoding("UTF-8");
        inOrder.verify(httpServletResponse).setStatus(SC_OK);
        inOrder.verify(out).println(errorJson);
        inOrder.verify(out).flush();
    }

    @Test
    public void shouldGetErrorJsonByErrorId() throws Exception {

        final UUID errorId = randomUUID();
        final String errorJson = "some-json";

        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
        final PrintWriter out = mock(PrintWriter.class);
        final List<StreamError> streamErrors = List.of(mock(StreamError.class));

        when(httpServletRequest.getParameter("streamId")).thenReturn(null);
        when(httpServletRequest.getParameter("errorId")).thenReturn(errorId.toString());
        when(streamErrorFinder.findByErrorId(errorId)).thenReturn(streamErrors);
        when(streamErrorsToJsonConverter.toJson(streamErrors)).thenReturn(errorJson);
        when(httpServletResponse.getWriter()).thenReturn(out);

        streamErrorServlet.doGet(httpServletRequest, httpServletResponse);

        final InOrder inOrder = inOrder(httpServletResponse, out);

        inOrder.verify(httpServletResponse).setContentType("application/json; charset=UTF-8");
        inOrder.verify(httpServletResponse).setCharacterEncoding("UTF-8");
        inOrder.verify(httpServletResponse).setStatus(SC_OK);
        inOrder.verify(out).println(errorJson);
        inOrder.verify(out).flush();
    }

    @Test
    public void shouldSend404BadRequestIfBothStreamIdAndErrorIdParametersArePresent() throws Exception {

        final UUID errorId = randomUUID();
        final UUID streamId = randomUUID();

        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);

        when(httpServletRequest.getParameter("streamId")).thenReturn(streamId.toString());
        when(httpServletRequest.getParameter("errorId")).thenReturn(errorId.toString());

        streamErrorServlet.doGet(httpServletRequest, httpServletResponse);

        verify(httpServletResponse).sendError(SC_BAD_REQUEST, "Please set either 'streamId' or 'errorId' as request parameters, not both");
        verifyNoInteractions(streamErrorFinder);
    }

    @Test
    public void shouldSend404BadRequestIfNeitherStreamIdNorErrorIdParametersArePresent() throws Exception {

        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);

        when(httpServletRequest.getParameter("streamId")).thenReturn(null);
        when(httpServletRequest.getParameter("errorId")).thenReturn(null);

        streamErrorServlet.doGet(httpServletRequest, httpServletResponse);

        verify(httpServletResponse).sendError(SC_BAD_REQUEST, "Please set either 'streamId' or 'errorId' as request parameters");
        verifyNoInteractions(streamErrorFinder);
    }
}