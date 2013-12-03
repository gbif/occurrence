package org.gbif.occurrence.cli;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrencestore.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.VerbatimOccurrencePersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.api.FragmentPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrencestore.processor.InterpretedProcessor;
import org.gbif.occurrencestore.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrencestore.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrencestore.processor.zookeeper.ZookeeperConnector;

import java.util.Collection;

import com.beust.jcommander.internal.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class InterpretedProcessorService extends AbstractIdleService {

  private final ProcessorConfiguration configuration;
  private MessageListener messageListener;
  private CuratorFramework curator;
  private final Collection<HTablePool> tablePools = Lists.newArrayList();

  public InterpretedProcessorService(ProcessorConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    configuration.ganglia.start();

    curator = configuration.zooKeeper.getCuratorFramework();
    ZookeeperConnector zkConnector = new ZookeeperConnector(curator);

    // we take the max connections from the user and divide over the number of tablepools we need to create
    int individualPoolSize = configuration.hbasePoolSize / 2;
    HTablePool tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    VerbatimOccurrencePersistenceService verbatimPersister =
      new VerbatimOccurrencePersistenceServiceImpl(configuration.occTable, tablePool);

    tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);
    OccurrencePersistenceService occurrenceService =
      new OccurrencePersistenceServiceImpl(configuration.occTable, tablePool);
    OccurrenceKeyPersistenceService keyService = new OccurrenceKeyPersistenceServiceImpl(
      new HBaseLockingKeyService(configuration.lookupTable, configuration.counterTable, configuration.occTable,
        tablePool));
    FragmentPersistenceService fragmentPersister =
      new FragmentPersistenceServiceImpl(configuration.occTable, tablePool, keyService);
    VerbatimOccurrenceInterpreter verbatimInterpreter =
      new VerbatimOccurrenceInterpreter(occurrenceService, zkConnector);
    InterpretedProcessor interpretedProcessor =
      new InterpretedProcessor(
        fragmentPersister,
        verbatimInterpreter,
        verbatimPersister,
        new DefaultMessagePublisher(configuration.messaging.getConnectionParameters()),
        zkConnector);
    messageListener = new MessageListener(configuration.messaging.getConnectionParameters());
    messageListener
      .listen(configuration.queueName, configuration.msgPoolSize, new VerbatimPersistedListener(interpretedProcessor));
  }

  @Override
  protected void shutDown() throws Exception {
    if (messageListener != null) {
      messageListener.close();
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
