package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.service.ScheduledService;
import org.gbif.occurrence.cli.common.DestroyCallback;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

/**
 * Service that runs {@link PreviousCrawlsManager} on a schedule.
 *
 */
public class ScheduledPreviousCrawlsManagerService extends ScheduledService {

  private final PreviousCrawlsManagerConfiguration config;
  private final PreviousCrawlsManager previousCrawlsManager;
  private final Consumer<Object> reportHandler;
  private DestroyCallback destroyCallback;

  /**
   * @param previousCrawlsManager
   * @param reportHandler
   * @param config
   */
  ScheduledPreviousCrawlsManagerService(PreviousCrawlsManager previousCrawlsManager,
                                        Consumer<Object> reportHandler,
                                        PreviousCrawlsManagerConfiguration config,
                                        DestroyCallback destroyCallback){
    this.previousCrawlsManager = previousCrawlsManager;
    this.reportHandler = reportHandler;
    this.config = config;
    this.destroyCallback = destroyCallback;
  }

  @Override
  protected void scheduledRun() {
    previousCrawlsManager.execute(reportHandler::accept);
  }

  @Override
  public void scheduledDestroy() {
    destroyCallback.destroy();
  }

  @Override
  protected LocalTime getTimeStart() {
    return config.scheduling.parseStartTime();
  }

  @Override
  protected int getIntervalInMinutes() {
    return (int)(ChronoUnit.HOURS.getDuration().toMinutes() * config.scheduling.frequencyInHour);
  }
}
