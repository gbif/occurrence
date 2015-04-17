package org.gbif.occurrence.cli;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class VerbatimProcessorCommand extends ServiceCommand {

  private final ProcessorCliConfiguration configuration = new ProcessorCliConfiguration();

  public VerbatimProcessorCommand() {
    super("verbatim-processor");
  }

  @Override
  protected Service getService() {
    return new VerbatimProcessorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
