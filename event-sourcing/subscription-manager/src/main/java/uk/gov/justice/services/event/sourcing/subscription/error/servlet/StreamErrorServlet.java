package uk.gov.justice.services.event.sourcing.subscription.error.servlet;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.UUID.fromString;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_OK;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "streamErrorServlet", urlPatterns = "/internal/stream-errors")
public class StreamErrorServlet extends HttpServlet {

    @Inject
    private StreamErrorFinder streamErrorFinder;

    @Inject
    private StreamErrorsToJsonConverter streamErrorsToJsonConverter;

    @SuppressWarnings("java:S1989")
    @Override
    protected void doGet(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse) throws ServletException, IOException {

        final Optional<String> streamId = ofNullable(httpServletRequest.getParameter("streamId"));
        final Optional<String> errorId = ofNullable(httpServletRequest.getParameter("errorId"));

        if (streamId.isPresent() && errorId.isPresent()) {
            httpServletResponse.sendError(SC_BAD_REQUEST, "Please set either 'streamId' or 'errorId' as request parameters, not both");
            return;
        }
        if (streamId.isEmpty() && errorId.isEmpty()) {
            httpServletResponse.sendError(SC_BAD_REQUEST, "Please set either 'streamId' or 'errorId' as request parameters");
            return;
        }

        final List<StreamError> errors = findErrors(streamId, errorId);
        final String json = streamErrorsToJsonConverter.toJson(errors);

        httpServletResponse.setContentType("application/json; charset=UTF-8");
        httpServletResponse.setCharacterEncoding("UTF-8");
        httpServletResponse.setStatus(SC_OK);

        final PrintWriter out = httpServletResponse.getWriter();
        out.println(json);
        out.flush();
    }

    private List<StreamError> findErrors(final Optional<String> streamId, final Optional<String> errorId) {

        if (streamId.isPresent()) {
            return streamErrorFinder.findByStreamId(fromString(streamId.get()));
        } else if (errorId.isPresent()) {
            return streamErrorFinder.findByErrorId(fromString(errorId.get()));
        }

        return emptyList();
    }
}
