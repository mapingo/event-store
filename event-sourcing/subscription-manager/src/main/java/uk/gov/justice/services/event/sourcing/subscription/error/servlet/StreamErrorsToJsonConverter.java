package uk.gov.justice.services.event.sourcing.subscription.error.servlet;

import uk.gov.justice.services.common.converter.ListToJsonArrayConverter;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;

public class StreamErrorsToJsonConverter {

    @Inject
    private ListToJsonArrayConverter<StreamError> streamErrorListToJsonArrayConverter;

    public String toJson(final List<StreamError> streamErrors) {

        final JsonArray convert = streamErrorListToJsonArrayConverter.convert(streamErrors);

        final Map<String, Object> properties = new HashMap<>(1);
        properties.put("javax.json.stream.JsonGenerator.prettyPrinting", true);
        final JsonWriterFactory writerFactory = Json.createWriterFactory(properties);

        final StringWriter stringWriter = new StringWriter();
        try (final JsonWriter jsonWriter = writerFactory.createWriter(stringWriter)) {
            jsonWriter.writeArray(convert);
            return stringWriter.toString();
        }
    }
}
