package org.gbif.occurrencestore.persistence.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrencestore.persistence.DatasetDeletionServiceImpl;
import org.gbif.occurrencestore.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.VerbatimOccurrencePersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.api.DatasetDeletionService;
import org.gbif.occurrencestore.persistence.api.FragmentPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrencestore.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrencestore.persistence.zookeeper.ZookeeperLockManager;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * A convenience module to include the OccurrencePersistenceServiceImpl via Guice. See the README for needed
 * properties.
 */
public class OccurrencePersistenceModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrencestore.db.";

  public OccurrencePersistenceModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    bind(OccurrenceService.class).to(OccurrencePersistenceServiceImpl.class);
    bind(OccurrencePersistenceService.class).to(OccurrencePersistenceServiceImpl.class);
    bind(OccurrenceKeyPersistenceService.class).to(OccurrenceKeyPersistenceServiceImpl.class);
    bind(VerbatimOccurrencePersistenceService.class).to(VerbatimOccurrencePersistenceServiceImpl.class);
    bind(FragmentPersistenceService.class).to(FragmentPersistenceServiceImpl.class);
    bind(ZookeeperLockManager.class).toProvider(ThreadLocalLockProvider.class);
    bind(new TypeLiteral<KeyPersistenceService<Integer>>(){}).to(HBaseLockingKeyService.class);
    bind(DatasetDeletionService.class).to(DatasetDeletionServiceImpl.class);

    expose(OccurrenceService.class);
    expose(OccurrencePersistenceService.class);
    expose(OccurrenceKeyPersistenceService.class);
    expose(VerbatimOccurrencePersistenceService.class);
    expose(FragmentPersistenceService.class);
    expose(ZookeeperLockManager.class);
    expose(new TypeLiteral<KeyPersistenceService<Integer>>(){});
    expose(DatasetDeletionService.class);
  }

  @Provides
  public HTablePool provideHTablePool(@Named("max_connection_pool") Integer maxConnectionPool) {
    return new HTablePool(HBaseConfiguration.create(), maxConnectionPool);
  }

  @Provides
  @Singleton
  public ThreadLocalLockProvider provideLockProvider(@Named("zookeeper.connection_string") String zkUrl) {
    CuratorFramework curator = CuratorFrameworkFactory.builder().namespace("hbasePersistence").connectString(zkUrl)
      .retryPolicy(new RetryNTimes(5, 1000)).build();
    curator.start();

    ThreadLocalLockProvider provider = new ThreadLocalLockProvider(curator);

    return provider;
  }
}
