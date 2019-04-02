package org.gbif.occurrence.processor;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sun.jersey.api.client.WebResource;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.util.MachineTagUtils;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.api.vocabulary.TagNamespace;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.processor.interpreting.util.RetryingWebserviceClient;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private final WebResource registryWs;

  private final Meter interpProcessed = Metrics.newMeter(
      InterpretedProcessor.class, "interps", "interps", TimeUnit.SECONDS);
  private final Timer interpTimer = Metrics.newTimer(
      InterpretedProcessor.class, "interp time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer msgTimer = Metrics.newTimer(
      InterpretedProcessor.class, "msg send time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedProcessor.class);

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final LoadingCache<WebResource, Dataset> DATASET_CACHE =
    CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(Dataset.class, 5, 2000));

  private static final LoadingCache<WebResource, Optional<Map<Term,String>>> DEFAULT_VALUE_CACHE =
    CacheBuilder.newBuilder()
      .maximumSize(10_000)
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .build(
        new CacheLoader<WebResource, Optional<Map<Term, String>>>() {
          @Override
          public Optional<Map<Term, String>> load(WebResource key) throws Exception {
            Dataset dataset = DATASET_CACHE.get(key);

            if (dataset != null) {
              List<MachineTag> mts = MachineTagUtils.list(dataset, TagNamespace.GBIF_DEFAULT_TERM);
              if (mts != null) {
                Map<Term, String> defaultsMap = new HashMap<>();
                for (MachineTag mt : mts) {
                  Term term = TERM_FACTORY.findPropertyTerm(mt.getName());
                  String defaultValue = mt.getValue();
                  if (term != null && !Strings.isNullOrEmpty(defaultValue)) {
                    defaultsMap.put(term, mt.getValue());
                  }
                }

                LOG.info("Dataset {} has verbatim defaults {}", dataset.getKey(), defaultsMap);
                return Optional.of(defaultsMap);
              } else {
                LOG.debug("Dataset {} has no verbatim defaults", dataset.getKey());
              }
            } else {
              LOG.warn("Dataset is null in verbatimDefaultsCache");
            }
            return Optional.empty();
          }
        }
      );

  @Inject
  public InterpretedProcessor(OccurrenceInterpreter occurrenceInterpreter, FragmentPersistenceService fragmentPersister,
     OccurrencePersistenceService occurrencePersister, MessagePublisher messagePublisher, ZookeeperConnector zookeeperConnector,
                              WebResource apiBaseWs) {
    this.occurrenceInterpreter = checkNotNull(occurrenceInterpreter, "occurrenceInterpreter can't be null");
    this.fragmentPersister = checkNotNull(fragmentPersister, "fragmentPersister can't be null");
    this.occurrencePersister = checkNotNull(occurrencePersister, "occurrencePersister can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
    this.zookeeperConnector = checkNotNull(zookeeperConnector, "zookeeperConnector can't be null");
    this.registryWs = checkNotNull(apiBaseWs, "apiBaseWs can't be null").path("");
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
  public void buildInterpreted(long occurrenceKey, OccurrencePersistenceStatus status, boolean fromCrawl,
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
      datasetKey = fragment.getDatasetKey();
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

    // Check for dataset machine tags specifying default term values
    if (datasetKey != null) {
      try {
        WebResource wr = registryWs.path("/dataset/"+datasetKey.toString());
        Optional<Map<Term, String>> verbatimDefaults = DEFAULT_VALUE_CACHE.get(wr);

        if (verbatimDefaults.isPresent()) {
          LOG.debug("Applying verbatim defaults {} to {}", verbatimDefaults.get(), verbatim);
          applyDefaults(verbatim, verbatimDefaults.get());
          LOG.debug("Applied verbatim defaults: {}", verbatim);
        }
      } catch (Exception e) {
        LOG.error("Exception when applying verbatim defaults", e);
        logError("Could not apply verbatim defaults", occurrenceKey, datasetKey, fromCrawl);
        return;
      }
    } else {
      LOG.info("Can't find verbatim defaults without a dataset key; occurrenceKey {}", occurrenceKey);
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

  /**
   * If a verbatim term is blank or empty, set it to the defined default.
   */
  private VerbatimOccurrence applyDefaults(VerbatimOccurrence verbatimOccurrence, Map<Term, String> defaults) {
    defaults.forEach((term, defaultValue) -> {
      if (!verbatimOccurrence.hasVerbatimField(term)) verbatimOccurrence.setVerbatimField(term, defaultValue);
    });
    return verbatimOccurrence;
  }

  private void logError(String message, long occurrenceKey, UUID datasetKey, boolean fromCrawl) {
    // TODO: send msg?
    LOG.error("{} verbatim occurrence with key [{}] - skipping.", message, occurrenceKey);
    if (fromCrawl) {
      LOG.debug("Updating zookeeper for InterpretedOccurrencePersistedError");
      zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR);
    }
  }
}
