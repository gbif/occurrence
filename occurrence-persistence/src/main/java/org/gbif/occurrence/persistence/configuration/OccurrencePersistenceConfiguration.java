package org.gbif.occurrence.persistence.configuration;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

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
