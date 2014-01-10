package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.OccurrencePersistenceStatus;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.VerbatimPersistedMessage;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.VerbatimOccurrence;
import org.gbif.occurrence.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrence.processor.parsing.FragmentParser;
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
 * Takes occurrence Fragments and parses them to produce and persist VerbatimOccurrence objects.
 */
@Singleton
public class VerbatimProcessor {

  private final FragmentPersistenceService fragmentPersister;
  private final VerbatimOccurrencePersistenceService verbatimPersister;
  private final MessagePublisher messagePublisher;
  private final ZookeeperConnector zookeeperConnector;

  private final Meter verbProcessed = Metrics.newMeter(VerbatimProcessor.class, "verbs", "verbs", TimeUnit.SECONDS);
  private final Timer msgTimer =
    Metrics.newTimer(VerbatimProcessor.class, "msg send time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  private static final Logger LOG = LoggerFactory.getLogger(VerbatimProcessor.class);

  @Inject
  public VerbatimProcessor(FragmentPersistenceService fragmentPersister,
    VerbatimOccurrencePersistenceService verbatimPersister, MessagePublisher messagePublisher,
    ZookeeperConnector zookeeperConnector) {
    this.fragmentPersister = checkNotNull(fragmentPersister, "fragmentPersister can't be null");
    this.verbatimPersister = checkNotNull(verbatimPersister, "verbatimPersister can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
    this.zookeeperConnector = checkNotNull(zookeeperConnector, "zookeeperConnector can't be null");
  }

  /**
   * Builds and persists a VerbatimOccurrence object by parsing an existing Fragment with the given occurrenceKey.
   * Updated zookeeper with success/error counts and sends a VerbatimPersistedMessage when successfully completed. Note
   * that UNCHANGED Fragments are ignored.
   *
   * @param occurrenceKey the key of the existing Fragment to be parsed
   * @param status        whether the Fragment is NEW, UPDATED, or UNCHANGED
   * @param fromCrawl     true if this method is called as part of a crawl
   * @param attemptId     the crawl attempt id, only used for passing along in logs and subsequent messages.
   * @param datasetKey    the dataset that this occurrence belongs to (must not be null if fromCrawl is true)
   */
  public void buildVerbatim(int occurrenceKey, OccurrencePersistenceStatus status, boolean fromCrawl,
    @Nullable Integer attemptId, @Nullable UUID datasetKey) {
    checkArgument(occurrenceKey > 0, "occurrenceKey must be greater than 0");
    checkNotNull(status, "status can't be null");
    if (fromCrawl) {
      checkNotNull(datasetKey, "datasetKey can't be null if fromCrawl is true");
      checkArgument(attemptId != null && attemptId > 0, "attemptId must be greater than 0 if fromCrawl is true");
    }

    if (status == OccurrencePersistenceStatus.UNCHANGED) {
      LOG.debug("Ignoring fragment of status UNCHANGED.");
      return;
    }

    Fragment fragment = fragmentPersister.get(occurrenceKey);

    if (fragment == null) {
      logError("Could not find", occurrenceKey, datasetKey, fromCrawl);
      return;
    }

    int localAttemptId = fromCrawl ? attemptId : fragment.getCrawlId();
    LOG.debug("Fragment for key [{}] and UUID [{}] crawl [{}] is [{}]", occurrenceKey, fragment.getDatasetKey(),
      localAttemptId, status);

    VerbatimOccurrence verbatim = FragmentParser.parse(fragment);
    if (verbatim == null) {
      // parsing has failed, skip this fragment
      logError("Could not parse", occurrenceKey, datasetKey, fromCrawl);
      return;
    }

    verbatimPersister.update(verbatim);

    if (fromCrawl) {
      LOG.debug("Updating zookeeper for VerbatimOccurrencePersistedSuccess");
      zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS);
    }

    VerbatimPersistedMessage verbMsg =
      new VerbatimPersistedMessage(verbatim.getDatasetKey(), localAttemptId, status, verbatim.getKey());
    final TimerContext msgContext = msgTimer.time();
    try {
      messagePublisher.send(verbMsg);
    } catch (IOException e) {
      LOG.warn("Could not send VerbatimPersistedMessage for successful [{}]", status, e);
    } finally {
      msgContext.stop();
    }

    verbProcessed.mark();
  }

  private void logError(String message, int occurrenceKey, UUID datasetKey, boolean fromCrawl) {
    // TODO: send msg?
    LOG.warn(message + " fragment with key [{}] - skipping.", occurrenceKey);
    if (fromCrawl) {
      LOG.debug("Updating zookeeper for VerbatimOccurrencePersistedError");
      zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR);
    }
  }
}
