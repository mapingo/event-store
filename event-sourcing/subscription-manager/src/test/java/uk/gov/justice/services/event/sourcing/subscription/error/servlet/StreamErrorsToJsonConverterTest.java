package uk.gov.justice.services.event.sourcing.subscription.error.servlet;

import static com.jayway.jsonassert.JsonAssert.with;
import static java.time.ZoneOffset.UTC;
import static java.util.Optional.empty;
import static java.util.UUID.fromString;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import uk.gov.justice.services.common.converter.ListToJsonArrayConverter;
import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHash;

import java.time.ZonedDateTime;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamErrorsToJsonConverterTest {


    @Spy
    private ListToJsonArrayConverter<StreamError> streamErrorListToJsonArrayConverter = new ListToJsonArrayConverter<>();

    @InjectMocks
    private StreamErrorsToJsonConverter streamErrorsToJsonConverter;

    @BeforeEach
    public void setup() {

        final ObjectMapper objectMapper = new ObjectMapperProducer().objectMapper();
        final StringToJsonObjectConverter stringToJsonObjectConverter = new StringToJsonObjectConverter();

        setField(streamErrorListToJsonArrayConverter, "mapper", objectMapper);
        setField(streamErrorListToJsonArrayConverter, "stringToJsonObjectConverter", stringToJsonObjectConverter);
    }


    @Test
    public void shouldConvertStreamErrorListToJsonArray() throws Exception {

        final List<StreamError> someStreamErrors = createSomeStreamErrors();

        final String json = streamErrorsToJsonConverter.toJson(someStreamErrors);
        with(json)
                .assertEquals("$[0].streamErrorDetails.id", "41a14e88-4151-4a37-bc78-b5b27b67b33f")
                .assertEquals("$[0].streamErrorDetails.hash", "some-hash-1")
                .assertEquals("$[0].streamErrorDetails.exceptionMessage", "exception-message-1")
                .assertEquals("$[0].streamErrorDetails.eventName", "some-event-name-1")
                .assertEquals("$[0].streamErrorDetails.eventId", "12321a93-b955-439d-9f64-f9ba6c70e37d")
                .assertEquals("$[0].streamErrorDetails.streamId", "b8c14c11-476c-425d-9161-7b834ff379ec")
                .assertEquals("$[0].streamErrorDetails.positionInStream", 23)
                .assertEquals("$[0].streamErrorDetails.dateCreated", "2025-06-30T21:22:00.000Z")
                .assertEquals("$[0].streamErrorDetails.fullStackTrace", "stack-trace-1")
                .assertEquals("$[0].streamErrorDetails.componentName", "some-component")
                .assertEquals("$[0].streamErrorDetails.source", "some-source")

                .assertEquals("$[0].streamErrorHash.hash", "some-hash")
                .assertEquals("$[0].streamErrorHash.exceptionClassName", "exception-class-name")
                .assertEquals("$[0].streamErrorHash.javaClassName", "java-class-name")
                .assertEquals("$[0].streamErrorHash.javaMethod", "java-method")
                .assertEquals("$[0].streamErrorHash.javaLineNumber", 76)

                .assertEquals("$[1].streamErrorDetails.id", "d7b0eb38-694d-4d09-a922-fc9f664219a3")
                .assertEquals("$[1].streamErrorDetails.hash", "some-hash-2")
                .assertEquals("$[1].streamErrorDetails.exceptionMessage", "exception-message-2")
                .assertEquals("$[1].streamErrorDetails.eventName", "some-event-name-2")
                .assertEquals("$[1].streamErrorDetails.eventId", "bcceb752-6411-489e-88ce-f5e25a0d15ea")
                .assertEquals("$[1].streamErrorDetails.streamId", "54e77cb6-47eb-4d90-a413-f27cde8b99fc")
                .assertEquals("$[1].streamErrorDetails.positionInStream", 24)
                .assertEquals("$[1].streamErrorDetails.dateCreated", "2025-06-30T21:11:00.000Z")
                .assertEquals("$[1].streamErrorDetails.fullStackTrace", "stack-trace-2")
                .assertEquals("$[1].streamErrorDetails.componentName", "some-component")
                .assertEquals("$[1].streamErrorDetails.source", "some-source")

                .assertEquals("$[1].streamErrorHash.hash", "some-hash")
                .assertEquals("$[1].streamErrorHash.exceptionClassName", "exception-class-name")
                .assertEquals("$[1].streamErrorHash.javaClassName", "java-class-name")
                .assertEquals("$[1].streamErrorHash.javaMethod", "java-method")
                .assertEquals("$[1].streamErrorHash.javaLineNumber", 76)
        ;
    }

    private List<StreamError> createSomeStreamErrors() {

        final StreamErrorDetails streamErrorDetails_1 = new StreamErrorDetails(
                fromString("41a14e88-4151-4a37-bc78-b5b27b67b33f"),
                "some-hash-1",
                "exception-message-1",
                empty(),
                "some-event-name-1",
                fromString("12321a93-b955-439d-9f64-f9ba6c70e37d"),
                fromString("b8c14c11-476c-425d-9161-7b834ff379ec"),
                23L,
                ZonedDateTime.of(2025, 6, 30, 21, 22, 0, 0, UTC),
                "stack-trace-1",
                "some-component",
                "some-source"
        );
        final StreamErrorDetails streamErrorDetails_2 = new StreamErrorDetails(
                fromString("d7b0eb38-694d-4d09-a922-fc9f664219a3"),
                "some-hash-2",
                "exception-message-2",
                empty(),
                "some-event-name-2",
                fromString("bcceb752-6411-489e-88ce-f5e25a0d15ea"),
                fromString("54e77cb6-47eb-4d90-a413-f27cde8b99fc"),
                24L,
                ZonedDateTime.of(2025, 6, 30, 21, 11, 0, 0, UTC),
                "stack-trace-2",
                "some-component",
                "some-source"
        );

        final StreamErrorHash streamErrorHash = new StreamErrorHash(
                "some-hash",
                "exception-class-name",
                empty(),
                "java-class-name",
                "java-method",
                76);


        return List.of(
                new StreamError(streamErrorDetails_1, streamErrorHash),
                new StreamError(streamErrorDetails_2, streamErrorHash)
        );
    }
}