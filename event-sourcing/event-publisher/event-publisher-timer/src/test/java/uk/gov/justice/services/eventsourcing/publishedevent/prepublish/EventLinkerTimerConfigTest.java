package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventLinkerTimerConfigTest {

    @InjectMocks
    private EventLinkerTimerConfig eventLinkerTimerConfig;

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {

        final long milliseconds = 982374L;

        setField(eventLinkerTimerConfig, "timerStartWaitMilliseconds", "" + milliseconds);

        assertThat(eventLinkerTimerConfig.getTimerStartWaitMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {

        final long milliseconds = 2998734L;

        setField(eventLinkerTimerConfig, "timerIntervalMilliseconds", "" + milliseconds);

        assertThat(eventLinkerTimerConfig.getTimerIntervalMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerMaxRuntime() throws Exception {

        final long milliseconds = 600L;

        setField(eventLinkerTimerConfig, "timerMaxRuntimeMilliseconds", "" + milliseconds);

        assertThat(eventLinkerTimerConfig.getTimerMaxRuntimeMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldReturnTrueIfDisabled() throws Exception {

        setField(eventLinkerTimerConfig, "disablePrePublish", "TRUE");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(true));

        setField(eventLinkerTimerConfig, "disablePrePublish", "true");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(true));

        setField(eventLinkerTimerConfig, "disablePrePublish", "True");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(true));

        setField(eventLinkerTimerConfig, "disablePrePublish", "FALSE");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(false));

        setField(eventLinkerTimerConfig, "disablePrePublish", "false");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(false));

        setField(eventLinkerTimerConfig, "disablePrePublish", "False");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(false));

        setField(eventLinkerTimerConfig, "disablePrePublish", "something very silly");
        assertThat(eventLinkerTimerConfig.isDisabled(), is(false));

        setField(eventLinkerTimerConfig, "disablePrePublish", null);
        assertThat(eventLinkerTimerConfig.isDisabled(), is(false));

        eventLinkerTimerConfig.setDisabled(true);

        assertThat(eventLinkerTimerConfig.isDisabled(), is(true));
    }
}
