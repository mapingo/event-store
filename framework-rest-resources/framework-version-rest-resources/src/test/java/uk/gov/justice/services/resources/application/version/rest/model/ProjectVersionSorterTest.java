package uk.gov.justice.services.resources.application.version.rest.model;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.services.common.util.UtcClock;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProjectVersionSorterTest {

    @InjectMocks
    private ProjectVersionSorter projectVersionSorter;

    @Test
    public void shouldSortTheListOfProjectVersionsByDisplayOrderAscending() throws Exception {

        final ProjectVersion projectVersion_1 = new ProjectVersion(
                "project-name-1",
                1,
                "project-version-1",
                new UtcClock().now()
        );
        final ProjectVersion projectVersion_2 = new ProjectVersion(
                "project-name-2",
                2,
                "project-version-2",
                new UtcClock().now()
        );
        final ProjectVersion projectVersion_3 = new ProjectVersion(
                "project-name-3",
                3,
                "project-version-3",
                new UtcClock().now()
        );

        final List<ProjectVersion> outOfOrderProjectVersion = new ArrayList<>();
        outOfOrderProjectVersion.add(projectVersion_2);
        outOfOrderProjectVersion.add(projectVersion_1);
        outOfOrderProjectVersion.add(projectVersion_3);

        assertThat(outOfOrderProjectVersion.get(0), is(projectVersion_2));

        final List<ProjectVersion> projectVersions = projectVersionSorter.sortByDisplayOrder(outOfOrderProjectVersion);

        assertThat(projectVersions.get(0), is(projectVersion_1));
        assertThat(projectVersions.get(1), is(projectVersion_2));
        assertThat(projectVersions.get(2), is(projectVersion_3));
    }
}