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
package org.gbif.occurrence.persistence.configuration;

import org.gbif.occurrence.common.config.OccHBaseConfiguration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.google.common.base.Throwables;

/**
 * A convenience module to include the OccurrencePersistenceServiceImpl via Guice. See the README for needed
 * properties.
 */
public class OccurrencePersistenceConfiguration {

  @ConfigurationProperties(prefix = "occurrence.db")
  @Bean
  public OccHBaseConfiguration occHBaseConfiguration() {
    return new OccHBaseConfiguration();
  }

  @Bean
  public Connection hBaseConnection(@Autowired(required = false) @Qualifier("hbaseConfiguration") Configuration hbaseConfiguration) {
    try {
      if (hbaseConfiguration != null) {
        return ConnectionFactory.createConnection(hbaseConfiguration);
      }
      return ConnectionFactory.createConnection(HBaseConfiguration.create());
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }
}
