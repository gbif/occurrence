package org.gbif.occurrence.cli.dataset;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/** Entry class for CLI command to start a service that updates ES when a dataset is deleted. */
@MetaInfServices(Command.class)
public class EsDatasetDeleterCommand extends ServiceCommand {

  private final EsDatasetDeleterConfiguration configuration =
      new EsDatasetDeleterConfiguration();

  public EsDatasetDeleterCommand() {
    super("es-dataset-deleter");
  }

  @Override
  protected Service getService() {
    return new EsDatasetDeleterService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
