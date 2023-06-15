package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configuration for the services used by the launcher. */
@Configuration
public class YamlConfiguration {

  @ConfigurationProperties(prefix = "downloads")
  @Bean
  public DownloadServiceConfiguration downloadServiceConfiguration() {
    return new DownloadServiceConfiguration();
  }

  @ConfigurationProperties(prefix = "registry")
  @Bean
  public RegistryConfiguration registryConfiguration() {
    return new RegistryConfiguration();
  }
}
