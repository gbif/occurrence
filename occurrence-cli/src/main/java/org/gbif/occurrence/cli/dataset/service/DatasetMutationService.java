package org.gbif.occurrence.cli.dataset.service;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.cli.dataset.DeleteDatasetListener;
import org.gbif.occurrencestore.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrencestore.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrencestore.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrencestore.persistence.keygen.KeyPersistenceService;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Handles messages to parse a dataset (from xml/json to verbatim) and to interpret a dataset (from verbatim to
 * interpreted). Eventually should also handle deletions (which live in the delete package for now).
 */
public class DatasetMutationService extends AbstractIdleService {

  private final DatasetMutationConfiguration config;
  private MessageListener listener;
  private HTablePool tablePool;

  public DatasetMutationService(DatasetMutationConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    config.ganglia.start();

    tablePool = new HTablePool(HBaseConfiguration.create(), config.hbasePoolSize);

    KeyPersistenceService<Integer> keyService =
      new HBaseLockingKeyService(config.lookupTable, config.counterTable, config.occTable, tablePool);

    OccurrenceKeyPersistenceService occurrenceService =
      new OccurrenceKeyPersistenceServiceImpl(keyService);

    // TODO: add listeners for parsing and interpreting
//    OccurrencePersistenceService occurrencePersistenceService =
//      new OccurrencePersistenceServiceImpl(config.occTable, tablePool);
//    FragmentPersistenceService fragmentService =
//      new FragmentPersistenceServiceImpl(config.occTable, tablePool, occurrenceService);
//    VerbatimOccurrencePersistenceService verbatimService =
//      new VerbatimOccurrencePersistenceServiceImpl(config.occTable, tablePool);

    listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.deleteDatasetQueueName, 1, new DeleteDatasetListener(occurrenceService,
      new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
    if (tablePool != null) {
      tablePool.close();
    }
  }
}
