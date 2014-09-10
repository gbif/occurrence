package org.gbif.occurrence.cli.dataset.service;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.cli.dataset.DeleteDatasetListener;
import org.gbif.occurrence.cli.dataset.InterpretDatasetListener;
import org.gbif.occurrence.cli.dataset.ParseDatasetListener;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Set;

import com.beust.jcommander.internal.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Handles messages to parse a dataset (from xml/json to verbatim) and to interpret a dataset (from verbatim to
 * interpreted). Eventually should also handle deletions (which live in the delete package for now).
 */
public class DatasetMutationService extends AbstractIdleService {

  private final DatasetMutationConfiguration config;
  private Set<MessageListener> listeners = Sets.newHashSet();
  private CuratorFramework curator;
  private HTablePool tablePool;

  public DatasetMutationService(DatasetMutationConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    config.ganglia.start();

    curator = config.zooKeeper.getCuratorFramework();
    ZookeeperConnector zkConnector = new ZookeeperConnector(curator);

    tablePool = new HTablePool(HBaseConfiguration.create(), config.hbasePoolSize);

    OccurrenceKeyPersistenceService keyService = new OccurrenceKeyPersistenceServiceImpl(
      new HBaseLockingKeyService(config.lookupTable, config.counterTable, config.occTable, tablePool));

    MessageListener listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.deleteDatasetQueueName, config.msgPoolSize,
      new DeleteDatasetListener(keyService, new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
    listeners.add(listener);

    listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.interpretDatasetQueueName, config.msgPoolSize, new InterpretDatasetListener(keyService,
      new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
    listeners.add(listener);

    listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.parseDatasetQueueName, config.msgPoolSize,
      new ParseDatasetListener(keyService, new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
    listeners.add(listener);

  }

  @Override
  protected void shutDown() throws Exception {
    for (MessageListener listener : listeners) {
      if (listener != null) {
        listener.close();
      }
    }
    if (curator != null) {
      curator.close();
    }
    if (tablePool != null) {
      tablePool.close();
    }
  }
}
