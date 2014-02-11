package org.gbif.occurrence.cli.dataset.commands;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class DatasetMutationConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--dataset-uuid")
  public String uuid;

  @Parameter(names = "--dataset-uuid-file")
  public String uuidFileName;
}
