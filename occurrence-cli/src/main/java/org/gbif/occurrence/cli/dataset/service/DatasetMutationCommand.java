package org.gbif.occurrence.cli.dataset.service;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class DatasetMutationCommand extends ServiceCommand {

  private final DatasetMutationConfiguration configuration = new DatasetMutationConfiguration();

  public DatasetMutationCommand() {
    super("occurrence-dataset-mutator");
  }

  @Override
  protected Service getService() {
    return new DatasetMutationService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
