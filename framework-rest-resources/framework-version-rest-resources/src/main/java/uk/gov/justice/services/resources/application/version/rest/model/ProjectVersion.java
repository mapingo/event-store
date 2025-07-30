package uk.gov.justice.services.resources.application.version.rest.model;

import java.time.ZonedDateTime;
import java.util.Objects;

public class ProjectVersion {

    private final String projectName;
    private final int displayOrder;
    private final String projectVersion;
    private final ZonedDateTime buildTimestamp;

    public ProjectVersion(
            final String projectName,
            final int displayOrder,
            final String projectVersion,
            final ZonedDateTime buildTimestamp) {
        this.projectName = projectName;
        this.displayOrder = displayOrder;
        this.projectVersion = projectVersion;
        this.buildTimestamp = buildTimestamp;
    }

    public String getProjectName() {
        return projectName;
    }

    public int getDisplayOrder() {
        return displayOrder;
    }

    public String getProjectVersion() {
        return projectVersion;
    }

    public ZonedDateTime getBuildTimestamp() {
        return buildTimestamp;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof final ProjectVersion that)) return false;
        return displayOrder == that.displayOrder && Objects.equals(projectName, that.projectName) && Objects.equals(projectVersion, that.projectVersion) && Objects.equals(buildTimestamp, that.buildTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectName, displayOrder, projectVersion, buildTimestamp);
    }

    @Override
    public String toString() {
        return "ProjectVersion{" +
               "projectName='" + projectName + '\'' +
               ", displayOrder=" + displayOrder +
               ", projectVersion='" + projectVersion + '\'' +
               ", buildTimestamp='" + buildTimestamp + '\'' +
               '}';
    }
}
