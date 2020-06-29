package org.gbif.occurrence.persistence.guice;

import com.google.common.base.Throwables;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.experimental.OccurrenceRelationshipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * A convenience module to include the OccurrencePersistenceServiceImpl via Guice. See the README for needed
 * properties.
 */
public class OccurrencePersistenceModule extends PrivateModule {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrencePersistenceModule.class);

  private final OccHBaseConfiguration cfg;
  private final Configuration hbaseConfiguration;

  @Deprecated
  public OccurrencePersistenceModule(Properties properties) {
    this(toCfg(properties));
  }

  public OccurrencePersistenceModule(OccHBaseConfiguration cfg) {
    this(cfg, null);
  }

  /**
   * Get an OccurrencePersistenceModule instance with the provided HBase configuration.
   * @param cfg
   * @param hbaseConfiguration
   */
  public OccurrencePersistenceModule(OccHBaseConfiguration cfg, Configuration hbaseConfiguration) {
    this.cfg = cfg;
    this.hbaseConfiguration = hbaseConfiguration;
  }

  private static OccHBaseConfiguration toCfg(Properties props) {
    OccHBaseConfiguration cfg = new OccHBaseConfiguration();
    try {
      cfg.fragmenterTable = props.getProperty("occurrence.db.fragmenter_name");
      cfg.relationshipTable = props.getProperty("occurrence.db.relationship_table_name");
      cfg.hbasePoolSize = Integer.parseInt(props.getProperty("occurrence.db.max_connection_pool"));
      cfg.zkConnectionString = props.getProperty("occurrence.db.zookeeper.connection_string");

      String fragmenterSalt = props.getProperty("occurrence.db.fragmenter_salt");
      if (fragmenterSalt != null && !fragmenterSalt.isEmpty()) {
        cfg.fragmenterSalt = Integer.parseInt(fragmenterSalt);
      }

      String relationshipSalt = props.getProperty("occurrence.db.relationship_table_salt");
      if (relationshipSalt != null && !relationshipSalt.isEmpty()) {
        cfg.relationshipSalt = Integer.parseInt(relationshipSalt);
      }

    } catch (RuntimeException e) {
      LOG.error("Occurrence persistence property configs invalid", e);
      Throwables.propagate(e);
    }
    return cfg;
  }

  @Override
  protected void configure() {
    bind(OccHBaseConfiguration.class).toInstance(cfg);
    bind(OccurrenceService.class).to(OccurrencePersistenceServiceImpl.class);
    bind(OccurrenceRelationshipService.class).to(OccurrencePersistenceServiceImpl.class);

    expose(OccurrenceService.class);
    expose(OccurrenceRelationshipService.class);
  }

  @Provides
  @Singleton
  public Connection provideHBaseConnection() {

    try {
      if(hbaseConfiguration != null){
        return ConnectionFactory.createConnection(hbaseConfiguration);
      }
      return ConnectionFactory.createConnection(HBaseConfiguration.create());
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }
}
