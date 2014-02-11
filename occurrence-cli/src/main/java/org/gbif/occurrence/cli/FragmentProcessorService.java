package org.gbif.occurrence.cli;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.processor.FragmentProcessor;
import org.gbif.occurrence.processor.messaging.OccurrenceFragmentedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Collection;

import com.beust.jcommander.internal.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class FragmentProcessorService extends AbstractIdleService {

  private final ProcessorConfiguration configuration;
  private CuratorFramework curator;
  private MessageListener listener;
  private final Collection<HTablePool> tablePools = Lists.newArrayList();

  public FragmentProcessorService(ProcessorConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    curator = configuration.zooKeeper.getCuratorFramework();
    ZookeeperConnector zookeeperConnector = new ZookeeperConnector(curator);

    configuration.ganglia.start();

    // we take the max connections from the user and divide over the number of tablepools we need to create
    int individualPoolSize = configuration.hbasePoolSize / 2;

    HTablePool tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    KeyPersistenceService<Integer> keyPersistenceService =
      new HBaseLockingKeyService(configuration.lookupTable, configuration.counterTable, configuration.occTable,
        tablePool);

    OccurrenceKeyPersistenceService occurrenceKeyPersister =
      new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    FragmentPersistenceService fragmentPersister =
      new FragmentPersistenceServiceImpl(configuration.occTable, tablePool, occurrenceKeyPersister);
    FragmentProcessor fragmentProcessor = new FragmentProcessor(fragmentPersister, occurrenceKeyPersister,
      new DefaultMessagePublisher(configuration.messaging.getConnectionParameters()), zookeeperConnector);

    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.primaryQueueName, configuration.msgPoolSize,
      new OccurrenceFragmentedListener(fragmentProcessor));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
    if (curator != null) {
      curator.close();
    }
    for (HTablePool pool : tablePools) {
      if (pool != null) {
        pool.close();
      }
    }
  }
}
