package uk.gov.justice.services.resources.application.version.rest.model;

import static java.util.Comparator.comparingInt;

import java.util.List;

public class ProjectVersionSorter {

    public List<ProjectVersion> sortByDisplayOrder(final List<ProjectVersion> versions) {
        versions.sort(comparingInt(ProjectVersion::getDisplayOrder));

        return versions;
    }
}
