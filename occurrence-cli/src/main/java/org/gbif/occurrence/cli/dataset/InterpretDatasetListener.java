package org.gbif.occurrence.cli.dataset;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.InterpretDatasetMessage;
import org.gbif.common.messaging.api.messages.InterpretVerbatimMessage;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class InterpretDatasetListener extends AbstractMessageCallback<InterpretDatasetMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretDatasetListener.class);

  private final OccurrenceKeyPersistenceService occurrenceKeyService;
  private final MessagePublisher messagePublisher;

  public InterpretDatasetListener(OccurrenceKeyPersistenceService occurrenceKeyService,
    MessagePublisher messagePublisher) {
    this.occurrenceKeyService = checkNotNull(occurrenceKeyService, "occurrenceKeyService can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
  }

  @Override
  public void handleMessage(InterpretDatasetMessage message) {
    LOG.info("Interpreting dataset for key [{}]", message.getDatasetUuid());

    Set<Integer> keys = occurrenceKeyService.findKeysByDataset(message.getDatasetUuid().toString());
    for (Integer key : keys) {
      try {
        messagePublisher.send(new InterpretVerbatimMessage(key));
      } catch (IOException e) {
        LOG.warn("Could not send InterpretVerbatimMessage for key [{}] while interpreting dataset [{}]", key,
          message.getDatasetUuid(), e);
      }
    }
  }
}
