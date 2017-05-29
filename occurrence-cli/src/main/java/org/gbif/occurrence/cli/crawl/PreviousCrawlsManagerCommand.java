package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.cli.common.JsonWriter;

import java.io.IOException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commands to manage occurrence record from previous crawls.
 */
@MetaInfServices(Command.class)
public class PreviousCrawlsManagerCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlsManagerCommand.class);
  private final PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();

  public PreviousCrawlsManagerCommand() {
    super("previous-crawls-manager");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    if (!checkConfiguration()) {
      return;
    }

    Injector injector = Guice.createInjector(new PreviousCrawlModule(config));
    PreviousCrawlsManager previousCrawlsManager = injector.getInstance(PreviousCrawlsManager.class);
    MessagePublisher messagePublisher = injector.getInstance(MessagePublisher.class);

    previousCrawlsManager.execute(this::printReport);

    //ensure we close the messagePublisher
    if (messagePublisher != null) {
      messagePublisher.close();
    }
  }

  /**
   * Makes configuration check on multiple parameters.
   *
   * @return
   */
  private boolean checkConfiguration() {
    if (config.delete && config.forceDelete) {
      System.err.println("--delete and --force-delete flags can not be used together");
      return false;
    }

    if (!config.displayReport && StringUtils.isBlank(config.reportOutputFilepath)) {
      System.err.println("--display-report or --report-output-filepath flag must be specified");
      return false;
    }
    return true;
  }

  /**
   * Print the report to a file or to the console depending on {@link PreviousCrawlsManagerConfiguration}.
   *
   * @param report
   */
  private void printReport(Object report) {
    try {
      if (StringUtils.isNotBlank(config.reportOutputFilepath)) {
        JsonWriter.objectToJsonFile(config.reportOutputFilepath, report);
      }

      if (config.displayReport) {
        System.out.print(JsonWriter.objectToJsonString(report));
      }
    } catch (IOException e) {
      LOG.error("Failed to write report.", e);
    }
  }

}





