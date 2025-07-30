package uk.gov.justice.services.resources.application.version.rest.resources;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersion;
import uk.gov.justice.services.resources.application.version.rest.model.ProjectVersionException;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/versions")
@Produces(APPLICATION_JSON)
public class FrameworkVersionsResource {

    @Inject
    private ProjectVersionsProvider projectVersionsProvider;

    @Inject
    private ObjectMapper objectMapper;

    @GET
    public Response getFrameworkVersions() {

        final List<ProjectVersion> projectVersions = projectVersionsProvider.findProjectVersions();

        try {
            final String json = createJsonStringAsWildflyCanNotParseZoneDateTime(projectVersions);
            return Response.ok(json, APPLICATION_JSON).build();
        } catch (final JsonProcessingException e) {
            throw new ProjectVersionException("Unable to serialize ProjectVersion List into json", e);
        }

    }

    private String createJsonStringAsWildflyCanNotParseZoneDateTime(final List<ProjectVersion> projectVersions) throws JsonProcessingException {
        return objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(projectVersions);
    }
}
