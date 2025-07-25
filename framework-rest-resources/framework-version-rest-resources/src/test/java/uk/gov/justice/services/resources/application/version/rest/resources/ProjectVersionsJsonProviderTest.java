package uk.gov.justice.services.resources.application.version.rest.resources;

import static com.jayway.jsonassert.JsonAssert.with;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProjectVersionsJsonProviderTest {

    @InjectMocks
    private ProjectVersionsJsonProvider projectVersionsJsonProvider;

    // Please note: this test all instances of the files 'project-version.json' found on the classpath.
    // currently these exist in event-store, framework and framework-libraries.
    @Test
    public void shouldLoadTheContentsOfAllProjectVersionJsonFilesFoundOnClasspath() throws Exception {

        final List<String> projectVersionsJson = projectVersionsJsonProvider.getProjectVersionsJson();

        projectVersionsJson.sort(String::compareTo);

        assertThat(projectVersionsJson.size(), is(3));

        with(projectVersionsJson.get(0)).assertThat("$.projectName", is("Event Store"));
        with(projectVersionsJson.get(1)).assertThat("$.projectName", is("Framework Libraries"));
        with(projectVersionsJson.get(2)).assertThat("$.projectName", is("Microservices Framework"));
    }
}