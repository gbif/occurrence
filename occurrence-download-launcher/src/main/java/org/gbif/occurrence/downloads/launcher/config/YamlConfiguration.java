package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class YamlConfiguration {

  @ConfigurationProperties(prefix = "downloads")
  @Bean
  public DownloadServiceConfiguration downloadServiceConfiguration() {
    return new DownloadServiceConfiguration();
  }

  @ConfigurationProperties(prefix = "spark")
  @Bean
  public SparkConfiguration sparkConfiguration() {
    return new SparkConfiguration();
  }

  @ConfigurationProperties(prefix = "registry")
  @Bean
  public RegistryConfiguration registryConfiguration() {
    return new RegistryConfiguration();
  }
}
