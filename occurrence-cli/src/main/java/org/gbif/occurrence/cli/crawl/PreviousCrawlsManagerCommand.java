package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.api.MessagePublisher;

import java.io.IOException;
import java.nio.file.Paths;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
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

    previousCrawlsManager.execute(this::printReportToJson);

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

    if (!config.displayReport && StringUtils.isBlank(config.reportLocation)) {
      System.err.println("--displayReport or --report-location flag must be specified");
      return false;
    }

    return true;
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





