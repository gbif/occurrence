package org.gbif.occurrence.persistence.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.DatasetDeletionServiceImpl;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.DatasetDeletionService;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.persistence.zookeeper.ZookeeperLockManager;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Throwables;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience module to include the OccurrencePersistenceServiceImpl via Guice. See the README for needed
 * properties.
 */
public class OccurrencePersistenceModule extends PrivateModule {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrencePersistenceModule.class);

  private final OccHBaseConfiguration cfg;

  @Deprecated
  public OccurrencePersistenceModule(Properties properties) {
    this(toCfg(properties));
  }

  public OccurrencePersistenceModule(OccHBaseConfiguration cfg) {
    this.cfg = cfg;
  }

  private static OccHBaseConfiguration toCfg(Properties props) {
    OccHBaseConfiguration cfg = new OccHBaseConfiguration();
    try {
      cfg.occTable = props.getProperty("occurrence.db.table_name");
      cfg.counterTable = props.getProperty("occurrence.db.counter_table_name");
      cfg.lookupTable = props.getProperty("occurrence.db.id_lookup_table_name");
      cfg.hbasePoolSize = Integer.valueOf(props.getProperty("occurrence.db.max_connection_pool"));
      cfg.zkConnectionString = props.getProperty("occurrence.db.zookeeper.connection_string");
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
    bind(OccurrencePersistenceService.class).to(OccurrencePersistenceServiceImpl.class);
    bind(OccurrenceKeyPersistenceService.class).to(OccurrenceKeyPersistenceServiceImpl.class);
    bind(FragmentPersistenceService.class).to(FragmentPersistenceServiceImpl.class);
    bind(ZookeeperLockManager.class).toProvider(ThreadLocalLockProvider.class);
    bind(new TypeLiteral<KeyPersistenceService<Integer>>() {
    }).to(HBaseLockingKeyService.class);
    bind(DatasetDeletionService.class).to(DatasetDeletionServiceImpl.class);

    expose(OccurrenceService.class);
    expose(OccurrencePersistenceService.class);
    expose(OccurrenceKeyPersistenceService.class);
    expose(FragmentPersistenceService.class);
    expose(ZookeeperLockManager.class);
    expose(new TypeLiteral<KeyPersistenceService<Integer>>() {
    });
    expose(DatasetDeletionService.class);
  }

  @Provides
  @Singleton
  public Connection provideHBaseConnection() {
    try {
      Configuration hBaseConfiguration = HBaseConfiguration.create();
      hBaseConfiguration.set("hbase.hconnection.threads.max", Integer.toString(cfg.hbasePoolSize));
      hBaseConfiguration.set("hbase.hconnection.threads.core", Integer.toString(1));
      return ConnectionFactory.createConnection(hBaseConfiguration);
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Provides
  @Singleton
  public ThreadLocalLockProvider provideLockProvider() {
    CuratorFramework curator =
      CuratorFrameworkFactory.builder().namespace("hbasePersistence").connectString(cfg.zkConnectionString)
        .retryPolicy(new RetryNTimes(5, 1000)).build();
    curator.start();

    return new ThreadLocalLockProvider(curator);
  }
}
