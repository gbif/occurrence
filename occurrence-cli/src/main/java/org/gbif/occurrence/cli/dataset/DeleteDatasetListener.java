package org.gbif.occurrence.cli.dataset;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.DeleteOccurrenceMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Translates a DeleteDatasetOccurrencesMessage into one DeleteOccurrenceMessage for every occurrence in the dataset,
 * but only if the deletion reason is not NOT_IN_LAST_CRAWL, which doesn't make sense in this context.
 */
public class DeleteDatasetListener extends AbstractMessageCallback<DeleteDatasetOccurrencesMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatasetListener.class);

  private final OccurrenceKeyPersistenceService occurrenceKeyService;
  private final MessagePublisher messagePublisher;

  private final Timer processTimer =
    Metrics.newTimer(DeleteDatasetListener.class, "dataset delete time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  public DeleteDatasetListener(OccurrenceKeyPersistenceService occurrenceKeyService,
    MessagePublisher messagePublisher) {
    this.occurrenceKeyService = checkNotNull(occurrenceKeyService, "occurrenceKeyService can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
  }

  @Override
  public void handleMessage(DeleteDatasetOccurrencesMessage message) {
    if (message.getDeletionReason() == OccurrenceDeletionReason.NOT_SEEN_IN_LAST_CRAWL) {
      LOG.warn("No support for deleting datasets because of NOT_SEEN_IN_LAST_CRAWL - ignoring message.");
      return;
    }

    final TimerContext context = processTimer.time();
    try {
      LOG.info("Deleting dataset for key [{}]", message.getDatasetUuid());

      Set<Integer> keys = occurrenceKeyService.findKeysByDataset(message.getDatasetUuid().toString());
      for (Integer key : keys) {
        try {
          messagePublisher.send(new DeleteOccurrenceMessage(key, message.getDeletionReason(), null, null));
        } catch (IOException e) {
          LOG.warn("Could not send DeleteOccurrenceMessage for key [{}] while deleting dataset [{}]", key,
            message.getDatasetUuid(), e);
        }
      }
    } finally {
      context.stop();
    }
  }
}
