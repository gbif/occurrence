package org.gbif.occurrence.cli.download.transfer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.StringJoiner;

/** Configuration required to upload GBIF downloads to cloud services. */
public class UploadToAzureConfiguration {

  @ParametersDelegate @Valid @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--download-key")
  @NotNull
  public String downloadKey;

  @Parameter(names = "--sas-token")
  @NotNull
  public String sasToken;

  @Parameter(names = "--endpoint")
  @NotNull
  public String endpoint;

  @Parameter(names = "--container-name")
  @NotNull
  public String containerName;

  @Override
  public String toString() {
    return new StringJoiner(
      ", ", UploadToAzureConfiguration.class.getSimpleName() + "[", "]")
        .add("sasToken=" + sasToken)
        .add("endpoint=" + endpoint)
        .add("containerName=" + containerName)
        .toString();
  }
}
