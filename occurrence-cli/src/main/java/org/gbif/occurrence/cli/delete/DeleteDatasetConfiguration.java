package org.gbif.occurrence.cli.delete;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class DeleteDatasetConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--dataset-uuid")
  @NotNull
  public String uuid;
}
