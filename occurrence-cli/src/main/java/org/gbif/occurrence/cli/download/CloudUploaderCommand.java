package org.gbif.occurrence.cli.download;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/** Entry class for CLI command to start a service that uploads GBIF downloads to cloud storage. */
@MetaInfServices(Command.class)
public class CloudUploaderCommand extends ServiceCommand {

  private final CloudUploaderConfiguration configuration =
      new CloudUploaderConfiguration();

  public CloudUploaderCommand() {
    super("cloud-uploader");
  }

  @Override
  protected Service getService() {
    return new CloudUploaderService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
