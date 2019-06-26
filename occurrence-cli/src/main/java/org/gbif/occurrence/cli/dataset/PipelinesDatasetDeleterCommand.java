package org.gbif.occurrence.cli.dataset;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/** Entry class for CLI command to start a service that updates ES when a dataset is deleted. */
@MetaInfServices(Command.class)
public class PipelinesDatasetDeleterCommand extends ServiceCommand {

  private final PipelinesDatasetDeleterConfiguration configuration =
      new PipelinesDatasetDeleterConfiguration();

  public PipelinesDatasetDeleterCommand() {
    super("pipelines-dataset-deleter");
  }

  @Override
  protected Service getService() {
    return new PipelinesDatasetDeleterService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
