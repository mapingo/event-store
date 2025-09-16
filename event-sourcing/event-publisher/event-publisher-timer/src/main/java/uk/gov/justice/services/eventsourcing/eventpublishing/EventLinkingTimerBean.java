package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerService;
import javax.inject.Inject;

@Singleton
@Startup
public class EventLinkingTimerBean {

    static final String TIMER_JOB_NAME = "event-store.link-new-events.job";

    @Resource
    private TimerService timerService;

    @Inject
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private EventLinkingWorker eventLinkingWorker;

    @Inject
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @PostConstruct
    public void startTimerService() {
        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                eventLinkingWorkerConfig.getTimerStartWaitMilliseconds(),
                eventLinkingWorkerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void runEventLinkingWorker(final Timer timer) {

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = sufficientTimeRemainingCalculatorFactory.createNew(
                timer,
                eventLinkingWorkerConfig.getTimeBetweenRunsMilliseconds());

        eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculator);
    }
}
