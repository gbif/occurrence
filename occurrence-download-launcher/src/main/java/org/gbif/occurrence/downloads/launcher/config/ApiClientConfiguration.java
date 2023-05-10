package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApiClientConfiguration {

  @Bean
  public OccurrenceDownloadClient occurrenceDownloadClient(RegistryConfiguration configuration) {
    return new ClientBuilder()
        .withUrl(configuration.getApiUrl())
        .withCredentials(configuration.getUserName(), configuration.getPassword())
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder()
        .build(OccurrenceDownloadClient.class);
  }
}
