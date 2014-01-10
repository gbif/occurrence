package org.gbif.occurrence.cli;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.VerbatimOccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.processor.VerbatimProcessor;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Collection;

import com.beust.jcommander.internal.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class VerbatimProcessorService extends AbstractIdleService {

  private final ProcessorConfiguration configuration;
  private MessageListener messageListener;
  private CuratorFramework curator;
  private final Collection<HTablePool> tablePools = Lists.newArrayList();

  public VerbatimProcessorService(ProcessorConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    configuration.ganglia.start();

    curator = configuration.zooKeeper.getCuratorFramework();
    ZookeeperConnector zkConnector = new ZookeeperConnector(curator);

    // we take the max connections from the user and divide over the number of tablepools we need to create
    int individualPoolSize = configuration.hbasePoolSize / 3;
    HTablePool tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    KeyPersistenceService keyPersistenceService =
      new HBaseLockingKeyService(configuration.lookupTable, configuration.counterTable, configuration.occTable,
        tablePool);
    OccurrenceKeyPersistenceService occurrenceKeyPersister =
      new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);

    tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    FragmentPersistenceService fragmentPersister =
      new FragmentPersistenceServiceImpl(configuration.occTable, tablePool, occurrenceKeyPersister);

    tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    VerbatimOccurrencePersistenceService verbatimPersister =
      new VerbatimOccurrencePersistenceServiceImpl(configuration.occTable, tablePool);
    VerbatimProcessor verbatimProcessor = new VerbatimProcessor(fragmentPersister, verbatimPersister,
      new DefaultMessagePublisher(configuration.messaging.getConnectionParameters()), zkConnector);

    messageListener = new MessageListener(configuration.messaging.getConnectionParameters());
    messageListener
      .listen(configuration.queueName, configuration.msgPoolSize, new FragmentPersistedListener(verbatimProcessor));
  }

  @Override
  protected void shutDown() throws Exception {
    if (messageListener != null) {
      messageListener.close();
    }
    curator.close();
    for (HTablePool pool : tablePools) {
      if (pool != null) {
        pool.close();
      }
    }
  }
}
