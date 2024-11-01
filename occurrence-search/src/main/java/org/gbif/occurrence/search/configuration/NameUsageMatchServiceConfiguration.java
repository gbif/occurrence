/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.search.configuration;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

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
