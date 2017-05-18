package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@MetaInfServices(Command.class)
public class ScheduledPreviousCrawlsCleanupCommand extends BaseCommand {
  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlsManagerCommand.class);
  private final PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();

  public ScheduledPreviousCrawlsCleanupCommand() {
    super("scheduled-previous-crawls-cleanup");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {

  }
}
