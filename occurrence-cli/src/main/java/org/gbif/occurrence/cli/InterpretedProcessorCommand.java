package org.gbif.occurrence.cli;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
public class InterpretedProcessorCommand extends ServiceCommand {

  private final ProcessorConfiguration configuration = new ProcessorConfiguration();

  public InterpretedProcessorCommand() {
    super("interpreted-processor");
  }

  @Override
  protected Service getService() {
    return new InterpretedProcessorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
