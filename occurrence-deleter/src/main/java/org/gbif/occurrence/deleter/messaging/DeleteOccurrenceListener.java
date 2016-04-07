package org.gbif.occurrence.deleter.messaging;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteOccurrenceMessage;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.occurrence.deleter.OccurrenceDeletionService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A MessageCallback implementation for {@link DeleteOccurrenceMessage}. Hands off to the
 * {@link org.gbif.occurrence.deleter.OccurrenceDeletionService} passed in during construction.
 */
public class DeleteOccurrenceListener extends AbstractMessageCallback<DeleteOccurrenceMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteOccurrenceListener.class);

  private final OccurrenceDeletionService occurrenceDeletionService;
  private final MessagePublisher messagePublisher;

  private final Timer processTimer =
    Metrics.newTimer(DeleteOccurrenceListener.class, "occurrence delete time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  public DeleteOccurrenceListener(OccurrenceDeletionService occurrenceDeletionService,
    MessagePublisher messagePublisher) {
    this.occurrenceDeletionService = checkNotNull(occurrenceDeletionService, "occurrenceDeletionService can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
  }

  @Override
  public void handleMessage(DeleteOccurrenceMessage message) {
    final TimerContext context = processTimer.time();
    try {
      Occurrence deleted = occurrenceDeletionService.deleteOccurrence(message.getOccurrenceKey());
      if (deleted != null) {
        try {
          messagePublisher.send(OccurrenceMutatedMessage
            .buildDeleteMessage(deleted.getDatasetKey(), deleted, message.getDeletionReason(),
              message.getCrawlAttemptLastSeen(), message.getLatestCrawlAttemptForDataset()));
        } catch (IOException e) {
          LOG.error("Unable to send OccurrenceDeletedMessage for key [{}] - ALL DOWNSTREAM COUNTS ARE NOW WRONG",
            deleted.getKey(), e);
        }
      }
    } finally {
      context.stop();
    }
  }
}

