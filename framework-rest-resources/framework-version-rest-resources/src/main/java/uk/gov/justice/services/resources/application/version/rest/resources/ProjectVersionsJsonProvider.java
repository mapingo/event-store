package uk.gov.justice.services.resources.application.version.rest.resources;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class ProjectVersionsJsonProvider {

    public static final String PROJECT_VERSION_JSON_FILE = "version/project-version.json";

    /**
     * Loads all instances of 'project-version-json' found on the classpath. There could be many of
     * these; one for event-store, framework, framework-libraries and file-store
     *
     * @return A List of json strings of the contents of all 'project-version.json' files found on
     * classpath
     */
    public List<String> getProjectVersionsJson() throws IOException {

        final Enumeration<URL> projectVersionJsonUrls = getClass().getClassLoader().getResources(PROJECT_VERSION_JSON_FILE);
        final List<String> projectVersionsJsonList = new ArrayList<>();
        while (projectVersionJsonUrls.hasMoreElements()) {
            final String json = IOUtils.toString(projectVersionJsonUrls.nextElement(), UTF_8);
            projectVersionsJsonList.add(json);
        }

        return projectVersionsJsonList;
    }

}
