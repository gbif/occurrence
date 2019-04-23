package org.gbif.occurrence.cli.process;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class InterpretOccurrenceConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--occurrence-key")
  public Long key;

  @Parameter(names = "--occurrence-key-file")
  public String keyFileName;
}
