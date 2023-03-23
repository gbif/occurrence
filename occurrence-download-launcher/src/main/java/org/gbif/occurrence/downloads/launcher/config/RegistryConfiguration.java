package org.gbif.occurrence.downloads.launcher.config;

import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class RegistryConfiguration {

  @NotNull
  private String apiUrl;
  @NotNull
  private String userName;
  @NotNull
  private String password;
}
