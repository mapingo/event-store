package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class StreamErrorHashRowMapper {

    public StreamErrorHash mapRow(ResultSet resultSet) throws SQLException {
        final String hash = resultSet.getString("hash");
        final String exceptionClassname = resultSet.getString("exception_classname");
        final String causeClassname = resultSet.getString("cause_classname");
        final String javaClassname = resultSet.getString("java_classname");
        final String javaMethod = resultSet.getString("java_method");
        final int javaLineNumber = resultSet.getInt("java_line_number");

        return new StreamErrorHash(
                hash,
                exceptionClassname,
                Optional.ofNullable(causeClassname),
                javaClassname,
                javaMethod,
                javaLineNumber
        );
    }
}
