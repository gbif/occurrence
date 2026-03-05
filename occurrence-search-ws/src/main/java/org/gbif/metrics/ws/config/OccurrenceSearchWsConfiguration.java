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
package org.gbif.metrics.ws.config;

import org.gbif.metrics.ws.service.CacheConfig;
import org.gbif.occurrence.search.configuration.OccurrenceSearchConfiguration;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(CacheConfig.class)
public class OccurrenceSearchWsConfiguration extends OccurrenceSearchConfiguration {

  @Bean
  public OccurrenceEsFieldMapper esFieldMapper() {
    return OccurrenceEsField.buildFieldMapper();
  }
}
