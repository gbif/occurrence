package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;

import org.kohsuke.MetaInfServices;

/**
 * Command to generate an occurrence crawls report.
 */
@MetaInfServices(Command.class)
public class CrawlsReportGeneratorCommand extends BaseCommand {

  private final CrawlsReportGeneratorConfiguration config = new CrawlsReportGeneratorConfiguration();

  public CrawlsReportGeneratorCommand() {
    super("generate-crawls-report");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    CrawlReportGeneratorService crawlReportGeneratorService = new CrawlReportGeneratorService(config);
    crawlReportGeneratorService.start();
  }
}





