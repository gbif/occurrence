package org.gbif.occurrence.cli.dataset.service;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.cli.dataset.DeleteDatasetListener;
import org.gbif.occurrence.cli.dataset.InterpretDatasetListener;
import org.gbif.occurrence.cli.dataset.ParseDatasetListener;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;

import java.util.Set;

import com.beust.jcommander.internal.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Handles messages to parse a dataset (from xml/json to verbatim) and to interpret a dataset (from verbatim to
 * interpreted). Eventually should also handle deletions (which live in the delete package for now).
 */
public class DatasetMutationService extends AbstractIdleService {

  private final DatasetMutationConfiguration config;
  private Set<MessageListener> listeners = Sets.newHashSet();

  public DatasetMutationService(DatasetMutationConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    Injector inj = Guice.createInjector(new OccurrencePersistenceModule(config.hbase));

    config.ganglia.start();

    OccurrenceKeyPersistenceService keyService = inj.getInstance(OccurrenceKeyPersistenceService.class);

    MessageListener listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.deleteDatasetQueueName, config.msgPoolSize,
        new DeleteDatasetListener(keyService,
        new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
    listeners.add(listener);

    listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.interpretDatasetQueueName, config.msgPoolSize,
      new InterpretDatasetListener(keyService,
      new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
    listeners.add(listener);

    listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.parseDatasetQueueName, config.msgPoolSize,
        new ParseDatasetListener(keyService,
        new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
    listeners.add(listener);

  }

  @Override
  protected void shutDown() throws Exception {
    for (MessageListener listener : listeners) {
      if (listener != null) {
        listener.close();
      }
    }
  }
}
