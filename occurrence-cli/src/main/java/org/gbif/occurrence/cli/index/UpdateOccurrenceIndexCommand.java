package org.gbif.occurrence.cli.index;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * Command for index updates and insertions processing.
 */
@MetaInfServices(Command.class)
public class UpdateOccurrenceIndexCommand extends ServiceCommand {

  private final IndexingConfiguration configuration = new IndexingConfiguration();

  public UpdateOccurrenceIndexCommand() {
    super("update-occurrence-index");
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

  @Override
  protected Service getService() {
    return new IndexUpdaterService(configuration);
  }
}