package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.occurrence.downloads.launcher.pojo.DistributedConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.StackableConfiguration;
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

  @ConfigurationProperties(prefix = "distributed")
  @Bean
  public DistributedConfiguration distributedConfiguration() {
    return new DistributedConfiguration();
  }

  @ConfigurationProperties(prefix = "spark")
  @Bean
  public SparkConfiguration sparkConfiguration() {
    return new SparkConfiguration();
  }

  @ConfigurationProperties(prefix = "stackable")
  @Bean
  public StackableConfiguration stackableConfiguration() {
    return new StackableConfiguration();
  }
}
