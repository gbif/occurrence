package org.gbif.occurrence.cli.delete.service;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.deleter.OccurrenceDeletionService;
import org.gbif.occurrence.deleter.messaging.DeleteOccurrenceListener;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class DeleterService extends AbstractIdleService {

  private final DeleterConfiguration config;
  private MessageListener listener;

  public DeleterService(DeleterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    Injector inj = Guice.createInjector(new OccurrencePersistenceModule(config.hbase));

    config.ganglia.start();

    OccurrenceDeletionService occurrenceDeletionService = new OccurrenceDeletionService(
      inj.getInstance(OccurrencePersistenceService.class), inj.getInstance(OccurrenceKeyPersistenceService.class));

    listener = new MessageListener(config.messaging.getConnectionParameters());
    listener.listen(config.queueName, config.msgPoolSize,
      new DeleteOccurrenceListener(occurrenceDeletionService,
      new DefaultMessagePublisher(config.messaging.getConnectionParameters())));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
  }
}
