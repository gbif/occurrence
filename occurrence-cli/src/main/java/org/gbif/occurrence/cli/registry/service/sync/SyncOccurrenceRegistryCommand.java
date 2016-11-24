package org.gbif.occurrence.cli.registry.service.sync;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;

import org.kohsuke.MetaInfServices;

/**
 * This command is executed once and finish.
 */
@MetaInfServices(Command.class)
public class SyncOccurrenceRegistryCommand extends BaseCommand {

  private final SyncOccurrenceRegistryConfiguration configuration = new SyncOccurrenceRegistryConfiguration();

  public SyncOccurrenceRegistryCommand() {
    super("sync-occurrence-registry");
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

  @Override
  protected void doRun() {
    SyncOccurrenceRegistryService service = new SyncOccurrenceRegistryService(configuration);
    service.doRun();
  }
}
