package uk.gov.justice.services.resources.application.version.rest;

import uk.gov.justice.services.resources.application.version.rest.resources.FrameworkVersionsResource;

import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("/internal/framework")
public class FrameworkVersioningApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        return Set.of(FrameworkVersionsResource.class);
    }
}
