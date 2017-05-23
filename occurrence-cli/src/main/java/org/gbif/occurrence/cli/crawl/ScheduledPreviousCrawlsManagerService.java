package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.service.ScheduledService;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

/**
 * Service that runs {@link PreviousCrawlsManager} on a schedule.
 *
 */
public class ScheduledPreviousCrawlsManagerService extends ScheduledService {

  private final PreviousCrawlsManagerConfiguration config;
  private ServiceProvider<PreviousCrawlsManager> previousCrawlsManagerServiceSupplier;

  /**
   * @param config
   * @param previousCrawlsManagerServiceSupplier Supplier capable of providing new instances
   */
  ScheduledPreviousCrawlsManagerService(PreviousCrawlsManagerConfiguration config,
                                        ServiceProvider<PreviousCrawlsManager> previousCrawlsManagerServiceSupplier){
    this.config = config;
    this.previousCrawlsManagerServiceSupplier = previousCrawlsManagerServiceSupplier;
  }

  @Override
  protected void scheduledRun() {
    PreviousCrawlsManager service = previousCrawlsManagerServiceSupplier.acquire();
    service.execute(previousCrawlsManagerServiceSupplier::handleReport);

    //release resources since we will sleep for a while
    previousCrawlsManagerServiceSupplier.release();
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
