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

import java.io.File;
import java.net.MalformedURLException;
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
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

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
  public HTablePool provideHTablePool() {
    File hbaseConfig = new File(cfg.hbaseConfig);
    checkArgument(hbaseConfig.exists() && hbaseConfig.isFile(), "hbase-site.xml does not exist");
    Configuration hadoopConfiguration = new Configuration();
    try {
      hadoopConfiguration.addResource(hbaseConfig.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("Unable to load hbase-site.xml from [{}] - configuration is broken.", hbaseConfig, e);
    }
    return new HTablePool(hadoopConfiguration, cfg.hbasePoolSize);
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
