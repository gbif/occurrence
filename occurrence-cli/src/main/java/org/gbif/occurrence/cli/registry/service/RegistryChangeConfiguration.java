package org.gbif.occurrence.cli.registry.service;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class RegistryChangeConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--registry-ws-url")
  @NotNull
  public String registryWsUrl;

  @Parameter(names = "--registry-change-queue-name")
  @NotNull
  public String registryChangeQueueName;

  @Parameter(names = "--registry-sync-properties", description = "Specify the location of the registry-sync.properties file")
  @NotNull
  public String registrySyncProperties;
}
