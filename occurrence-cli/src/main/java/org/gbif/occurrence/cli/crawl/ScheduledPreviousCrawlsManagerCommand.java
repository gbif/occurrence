package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.common.messaging.api.MessagePublisher;

import java.io.IOException;
import java.nio.file.Paths;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commands to manage occurrence record from previous crawls in a scheduled task.
 */
@MetaInfServices(Command.class)
public class ScheduledPreviousCrawlsManagerCommand extends ServiceCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledPreviousCrawlsManagerCommand.class);

  private final PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();

  public ScheduledPreviousCrawlsManagerCommand() {
    super("scheduled-previous-crawls-manager");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected Service getService() {
    return new ScheduledPreviousCrawlsManagerService(config, new ScheduledPreviousCrawlsManagerServiceProvider(config));
  }

  /**
   * Provides an indirect access to {@link PreviousCrawlsManager} so we can release resources between scheduled calls.
   * This is probably room for improvement here but at least it's simple.
   */
  private static class ScheduledPreviousCrawlsManagerServiceProvider implements ServiceProvider<PreviousCrawlsManager> {

    private final PreviousCrawlsManagerConfiguration config;
    private MessagePublisher messagePublisher;
    private PreviousCrawlsOccurrenceDeleter deletePreviousCrawlsService;
    private PreviousCrawlsManager previousCrawlsManagerService;

    ScheduledPreviousCrawlsManagerServiceProvider(PreviousCrawlsManagerConfiguration config) {
      this.config = config;
    }

    @Override
    public PreviousCrawlsManager acquire() {

      //ensure to release resource
      release();

      Injector injector = Guice.createInjector(new PreviousCrawlModule(config));
      previousCrawlsManagerService = injector.getInstance(PreviousCrawlsManager.class);
      messagePublisher = injector.getInstance(MessagePublisher.class);
      return previousCrawlsManagerService;
    }

    @Override
    public void handleReport(Object report) {
      printReportToJson(report);
    }

    @Override
    public void release() {
      if(messagePublisher != null) {
        messagePublisher.close();
      }
      messagePublisher = null;
      deletePreviousCrawlsService = null;
      previousCrawlsManagerService = null;
    }

    /**
     * Print the report to a file or to the console depending on {@link PreviousCrawlsManagerConfiguration}.
     * @param report
     */
    private void printReportToJson(Object report) {
      ObjectMapper om = new ObjectMapper();
      om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
      try {

        if (StringUtils.isNotBlank(config.reportLocation)) {
          om.writeValue(Paths.get(config.reportLocation).toFile(), report);
        }

        if (config.displayReport) {
          System.out.print(om.writeValueAsString(report));
        }
      } catch (IOException e) {
        LOG.error("Failed to write report.", e);
      }
    }
  }

}
