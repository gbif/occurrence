package org.gbif.occurrence.search.configuration;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "name-services")
@Primary
public class NameUsageMatchServiceConfiguration {

  List<NameUsageMatchServiceConfig> services;

  @Data
  public static class NameUsageMatchServiceConfig {
    private String name;
    private String datasetKey;
    private Ws ws;
    private String prefix;

    @Data
    public static class Ws {
      private Api api;

      @Data
      public static class Api {
        private String wsUrl;
      }
    }
  }
}
