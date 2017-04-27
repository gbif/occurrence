package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes VerbatimOccurrences and interprets their raw (String) fields into typed fields, producing Occurrence records.
 */
@Singleton
public class InterpretedProcessor {

  private final OccurrenceInterpreter occurrenceInterpreter;
  private final FragmentPersistenceService fragmentPersister;
  private final OccurrencePersistenceService occurrencePersister;
  private final MessagePublisher messagePublisher;
  private final ZookeeperConnector zookeeperConnector;

  private final Meter interpProcessed = Metrics.newMeter(
      InterpretedProcessor.class, "interps", "interps", TimeUnit.SECONDS);
  private final Timer interpTimer = Metrics.newTimer(
      InterpretedProcessor.class, "interp time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer msgTimer = Metrics.newTimer(
      InterpretedProcessor.class, "msg send time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedProcessor.class);

  @Inject
  public InterpretedProcessor(OccurrenceInterpreter occurrenceInterpreter, FragmentPersistenceService fragmentPersister,
     OccurrencePersistenceService occurrencePersister, MessagePublisher messagePublisher, ZookeeperConnector zookeeperConnector) {
    this.occurrenceInterpreter = checkNotNull(occurrenceInterpreter, "occurrenceInterpreter can't be null");
    this.fragmentPersister = checkNotNull(fragmentPersister, "fragmentPersister can't be null");
    this.occurrencePersister = checkNotNull(occurrencePersister, "occurrencePersister can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
    this.zookeeperConnector = checkNotNull(zookeeperConnector, "zookeeperConnector can't be null");
  }

  /**
   * Builds and persists an Occurrence record by interpreting the fields of the VerbatimOccurrence identified by the
   * passed in occurrenceKey. Note that UNCHANGED occurrences are ignored.
   *
   * @param occurrenceKey the key of the VerbatimOccurrence that will be fetched from HBase and interpreted
   * @param status        whether this is a NEW, UPDATED, or UNCHANGED occurrence
   * @param fromCrawl     true if this method is called as part of a crawl
   * @param attemptId     the crawl attempt id, only used for passing along in logs and subsequent messages.
   * @param datasetKey    the dataset that this occurrence belongs to (must not be null if fromCrawl is true)
   */
  public void buildInterpreted(int occurrenceKey, OccurrencePersistenceStatus status, boolean fromCrawl,
    @Nullable Integer attemptId, @Nullable UUID datasetKey) {
    checkArgument(occurrenceKey > 0, "occurrenceKey must be greater than 0");
    checkNotNull(status, "status can't be null");
    if (fromCrawl) {
      checkNotNull(datasetKey, "datasetKey can't be null if fromCrawl is true");
      checkArgument(attemptId != null && attemptId > 0, "attemptId must be greater than 0 if fromCrawl is true");
    }

    if (status == OccurrencePersistenceStatus.UNCHANGED) {
      LOG.debug("Ignoring verbatim of status UNCHANGED.");
      return;
    }

    int localAttemptId;
    if (fromCrawl) {
      localAttemptId = attemptId;
    } else {
      Fragment fragment = fragmentPersister.get(occurrenceKey);
      if (fragment == null) {
        LOG.warn(
          "Could not find fragment with key [{}] when looking up attemptId for non-crawl interpretation - skipping.",
          occurrenceKey);
        return;
      }
      localAttemptId = fragment.getCrawlId();
    }

    VerbatimOccurrence verbatim = occurrencePersister.getVerbatim(occurrenceKey);
    if (verbatim == null) {
      logError("Could not find", occurrenceKey, datasetKey, fromCrawl);
      return;
    }

    // Need the original for the interpretation result
    Occurrence original = null;
    if (status == OccurrencePersistenceStatus.UPDATED) {
      original = occurrencePersister.get(verbatim.getKey());
    }

    OccurrenceInterpretationResult interpretationResult;
    final TimerContext interpContext = interpTimer.time();
    try {
      interpretationResult = occurrenceInterpreter.interpret(verbatim, original);
    } finally {
      interpContext.stop();
    }

    // persist the record (considered an update in all cases because the key must already exist on verbatim)
    Occurrence interpreted = interpretationResult.getUpdated();
    LOG.debug("Persisting interpreted occurrence {}", interpreted);
    occurrencePersister.update(interpreted);

    if (fromCrawl) {
      LOG.debug("Updating zookeeper for OccurrenceInterpretedPersistedSuccess");
      zookeeperConnector
          .addCounter(interpreted.getDatasetKey(), ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    }

    // TODO: Compare original with newly interpreted
    // This approach won't work, since the lastInterpreted() dates differ.
    // if (interpreted.equals(original)) {
    //   status = OccurrencePersistenceStatus.UNCHANGED;
    // }

    OccurrenceMutatedMessage interpMsg;
    // can only be NEW or UPDATED
    switch (status) {
      case NEW:
        interpMsg = OccurrenceMutatedMessage.buildNewMessage(interpreted.getDatasetKey(), interpreted, localAttemptId);
        break;
      case UPDATED:
        interpMsg = OccurrenceMutatedMessage
            .buildUpdateMessage(interpreted.getDatasetKey(), interpretationResult.getOriginal(), interpreted,
                localAttemptId);
        break;
      case UNCHANGED: // Don't send any message.
      case DELETED: // Can't happen.
      default:
        interpMsg = null;
    }

    LOG.debug("Sending message (unless unchanged) {}", status);

    if (interpMsg != null) {
      final TimerContext context = msgTimer.time();
      try {
        messagePublisher.send(interpMsg);
      } catch (IOException e) {
        LOG.warn("Could not send OccurrencePersistedMessage for successful [{}]", status.toString(), e);
      } finally {
        context.stop();
      }
    }

    interpProcessed.mark();
  }

  private void logError(String message, int occurrenceKey, UUID datasetKey, boolean fromCrawl) {
    // TODO: send msg?
    LOG.error("{} verbatim occurrence with key [{}] - skipping.", message, occurrenceKey);
    if (fromCrawl) {
      LOG.debug("Updating zookeeper for InterpretedOccurrencePersistedError");
      zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR);
    }
  }
}
