package uk.gov.justice.services.eventsourcing.util.jee.timer;

import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import javax.inject.Inject;

public class TimerServiceManager {

    @Inject
    TimerConfigFactory timerConfigFactory;

    @Inject
    TimerCanceler timerCanceler;

    public void createIntervalTimer(
            final String timerJobName,
            final long timerStartWaitMilliseconds,
            final long timerIntervalMilliseconds,
            final TimerService timerService) {
        final TimerConfig timerConfig = timerConfigFactory.createNew();

        timerConfig.setPersistent(false);
        timerConfig.setInfo(timerJobName);

        timerService.createIntervalTimer(timerStartWaitMilliseconds, timerIntervalMilliseconds, timerConfig);
    }

    public void createSingleActionTimer(
            final String timerJobName,
            final long duration,
            final TimerService timerService) {
        final TimerConfig timerConfig = timerConfigFactory.createNew();

        timerConfig.setPersistent(false);
        timerConfig.setInfo(timerJobName);

        timerService.createSingleActionTimer(duration, timerConfig);
    }
}
