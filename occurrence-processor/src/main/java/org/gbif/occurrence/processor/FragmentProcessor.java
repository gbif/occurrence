package org.gbif.occurrence.processor;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.FragmentPersistedMessage;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.parsing.xml.IdentifierExtractionResult;
import org.gbif.occurrence.parsing.xml.XmlFragmentParser;
import org.gbif.occurrence.persistence.IllegalDataStateException;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentCreationResult;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.processor.identifiers.IdentifierStrategy;
import org.gbif.occurrence.processor.parsing.JsonFragmentParser;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.validation.ValidationException;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Responsible for producing and persisting Fragment objects given crawling response snippets, and sending a message
 * reporting successful persistence. Also updates ZooKeeper with running counts of success/failure.
 */
@Singleton
public class FragmentProcessor {

  // Please read POR-2807 before changing.
  // Previously been 3 times @ 500msecs, but this reduces crawling to 15/sec as there are 1.5 sec wait times.
  private static final int MAX_NULL_FRAG_RETRIES = 3;
  private static final long NULL_FRAG_RETRY_WAIT = 100;

  private final FragmentPersistenceService fragmentPersister;
  private final OccurrenceKeyPersistenceService occurrenceKeyPersister;
  private final MessagePublisher messagePublisher;
  private final ZookeeperConnector zookeeperConnector;

  private final Meter fragmentsProcessed =
    Metrics.newMeter(FragmentProcessor.class, "frags", "frags", TimeUnit.SECONDS);
  private final Timer persistenceTimer =
    Metrics.newTimer(FragmentProcessor.class, "frag persist time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer findTimer =
    Metrics.newTimer(FragmentProcessor.class, "frag find time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer xmlParsingTimer =
    Metrics.newTimer(FragmentProcessor.class, "xml parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer jsonParsingTimer =
    Metrics.newTimer(FragmentProcessor.class, "json parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer msgTimer =
    Metrics.newTimer(FragmentProcessor.class, "msg send time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Meter msgsSent = Metrics.newMeter(FragmentProcessor.class, "msgs", "msgs", TimeUnit.SECONDS);

  private static final Logger LOG = LoggerFactory.getLogger(FragmentProcessor.class);

  @Inject
  public FragmentProcessor(FragmentPersistenceService fragmentPersister,
    OccurrenceKeyPersistenceService occurrenceKeyPersister, MessagePublisher messagePublisher,
    ZookeeperConnector zookeeperConnector) {
    this.fragmentPersister = checkNotNull(fragmentPersister, "fragmentPersister can't be null");
    this.occurrenceKeyPersister = checkNotNull(occurrenceKeyPersister, "occurrenceKeyPersister can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
    this.zookeeperConnector = checkNotNull(zookeeperConnector, "zookeeperConnector can't be null");
  }

  /**
   * Takes the given response/dwca fragment and parses it into Fragment object(s) and then persists it/them.
   * Responsible for identifying duplicates of existing Fragments and assigning ids to new Fragments. The only case
   * where multiple Fragments are returned from this message is ABCD 2.06 snippets that are differentiated with
   * UnitQualifiers.
   *
   * @param datasetKey       the dataset to which this fragment belongs
   * @param data             a snippet of xml or json from a crawler response, representing a single "record"
   * @param schema           the crawler response type (e.g. ABCD_2_06, DWC_1_4) or DWCA for DwC-A as JSON
   * @param protocol         the endpoint type of this occurrence's dataset (e.g. DIGIR, BIOCASE, DWC_ARCHIVE)
   * @param crawlId          the crawl attempt that produced the snippet
   * @param validationReport the DwcaValidationReport if this fragment came from a DwC-A, null otherwise
   */
  public void buildFragments(UUID datasetKey, byte[] data, OccurrenceSchemaType schema, EndpointType protocol,
    Integer crawlId, @Nullable DwcaValidationReport validationReport) {
    checkNotNull(datasetKey, "datasetKey can't be null");
    checkNotNull(data, "data can't be null");
    checkNotNull(crawlId, "crawlId can't be null");
    checkNotNull(schema, "xmlSchema can't be null");
    checkNotNull(protocol, "protocol can't be null");
    if (schema == OccurrenceSchemaType.DWCA) {
      checkNotNull(validationReport, "validationReport can't be null if schema is DWCA");
    }

    MDC.put("datasetUuid", datasetKey.toString());
    MDC.put("attempt", crawlId.toString());

    boolean isXml = schema != OccurrenceSchemaType.DWCA;
    IdentifierStrategy idStrategy = new IdentifierStrategy(schema, validationReport);
    LOG.debug("For dataset [{}] of type [{}] useTriplet is [{}] and useOccurrenceId is [{}]",
      datasetKey, schema, idStrategy.isTripletsValid(), idStrategy.isOccurrenceIdsValid());

    // update zookeeper - fragment received but not yet processed
    zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED);

    Set<IdentifierExtractionResult> extractionResults;
    if (isXml) {
      final TimerContext context = xmlParsingTimer.time();
      try {
        extractionResults = XmlFragmentParser.extractIdentifiers(datasetKey, data, schema, idStrategy.isTripletsValid(),
          idStrategy.isOccurrenceIdsValid());
      } finally {
        context.stop();
      }
    } else {
      final TimerContext context = jsonParsingTimer.time();
      try {
        extractionResults = Sets.newHashSet();
        IdentifierExtractionResult result = JsonFragmentParser
          .extractIdentifiers(datasetKey, data, idStrategy.isTripletsValid(), idStrategy.isOccurrenceIdsValid());
        if (result != null) {
          extractionResults.add(result);
        }
      } finally {
        context.stop();
      }
    }

    if (extractionResults.isEmpty()) {
      LOG.debug(
        "Given snippet for dataset [{}] does not contain enough information to uniquely identify it - can not parse or "
        + "persist it", datasetKey.toString());
      LOG.debug("Raw snippet that couldn't be uniquely identified: [\n{}\n]",
        new String(data, Charset.forName("UTF-8")));
      zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
    } else {
      for (IdentifierExtractionResult extractionResult : extractionResults) {
        processSingleFragment(extractionResult.getUniqueIdentifiers(), isXml, data, schema, protocol, crawlId,
          extractionResult.getUnitQualifier());
      }
    }

    // update zookeeper with a single increment for this snippet (not for each Fragment)
    LOG.debug("Updating zookeeper for FragmentProcessed");
    zookeeperConnector.addCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_PROCESSED);
  }

  /**
   * Process a single record into a single Fragment and persist it, generating a new key for it if necessary.
   */
  private void processSingleFragment(Set<UniqueIdentifier> uniqueIds, boolean isXml, byte[] data,
    OccurrenceSchemaType schema, EndpointType protocol, Integer crawlId, String unitQualifier) {

    UUID datasetKey = uniqueIds.iterator().next().getDatasetKey();

    // check for an existing key for these uniqueIds
    KeyLookupResult keyResult = null;
    final TimerContext fetchContext = findTimer.time();
    try {
      keyResult = occurrenceKeyPersister.findKey(uniqueIds);
    } catch (IllegalDataStateException e) {
      failForDataStateException(datasetKey, e);
      return;
    } catch (ServiceUnavailableException e) {
      failForServiceUnavailable(datasetKey, e);
      return;
    } finally {
      fetchContext.stop();
    }

    Fragment fragment;
    OccurrencePersistenceStatus status;
    if (keyResult == null) {
      // this is a new record, prepare to insert a new Fragment
      fragment = new Fragment(datasetKey);
      status = OccurrencePersistenceStatus.NEW;
    } else {
      // this is an existing record - fetch fragment from hbase and see if we need to do an update
      int key = keyResult.getKey();
      int attempts = 0;
      do {
        attempts++;
        LOG.debug("Attempt [{}] to fetch fragment for key [{}]", attempts, key);
        try {
          fragment = fragmentPersister.get(key);
        } catch (ValidationException v) {
          LOG.warn("The fragment for key [{}] is invalid. Ignoring this update.", v);
          updateZookeeper(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
          return;
        } catch (ServiceUnavailableException e) {
          failForServiceUnavailable(datasetKey, e);
          return;
        }
        if (fragment == null) {
          /* this means either
              1) the key we're trying was only just assigned and the owner hasn't written their fragment yet, or
              2) our index is out of sync - it has an index entry pointing to nothing, so delete the index
                entry(ies) and proceed as if this is an insert
             So we retry a few times to get clear of 1), and then act on 2).
             Note however in POR-2807, that if for some reason deletions are not clearing lookups (i.e. POR-995)
             too conservative rates mean the system grinds to a complete halt for large datasets.
          */
          LOG.debug("Fragment for key [{}] was null, sleeping and trying again", key);
          try {
            Thread.sleep(NULL_FRAG_RETRY_WAIT);
          } catch (InterruptedException e) {
            LOG.info("Woken up from sleep while waiting to retry fragment get", e);
          }
        }
      } while (fragment == null && attempts <= MAX_NULL_FRAG_RETRIES);

      if (fragment == null) {
        // we're in case 2) from above
        LOG.info("Could not retrieve Fragment with key [{}] even though it exists in lookup table - deleting lookup "
                 + "and inserting Fragment as NEW.", key);
        try {
          occurrenceKeyPersister.deleteKeyByUniqueIdentifiers(uniqueIds);
        } catch (ServiceUnavailableException e) {
          failForServiceUnavailable(datasetKey, e);
          return;
        }
        fragment = new Fragment(datasetKey);
        status = OccurrencePersistenceStatus.NEW;
      } else {
        status = OccurrencePersistenceStatus.UPDATED;
      }
    }

    // check hashes to see if this is an existing and unchanged record
    byte[] newHash = DigestUtils.md5(data);
    if (status == OccurrencePersistenceStatus.UPDATED && Arrays.equals(newHash, fragment.getDataHash())) {
      // the new record and existing record don't differ
      status = OccurrencePersistenceStatus.UNCHANGED;
    } else {
      // this is an UPDATE or NEW, but not UNCHANGED
      fragment.setData(data);
      fragment.setXmlSchema(schema);
      fragment.setProtocol(protocol);
      fragment.setDataHash(newHash);
      fragment.setHarvestedDate(new Date());
      fragment.setCrawlId(crawlId);
      fragment.setUnitQualifier(unitQualifier);
      if (isXml) {
        fragment.setFragmentType(Fragment.FragmentType.XML);
      } else {
        fragment.setFragmentType(Fragment.FragmentType.JSON);
      }
    }

    // update these in every case (including UNCHANGED)
    fragment.setHarvestedDate(new Date());
    fragment.setCrawlId(crawlId);

    if (LOG.isDebugEnabled()) {
      if (status != OccurrencePersistenceStatus.NEW) {
        StringBuilder uniques = new StringBuilder(64);

        for (UniqueIdentifier uniqueIdentifier : uniqueIds) {
          if (uniques.length() > 0) {
            uniques.append(" ||| ");
          }
          uniques.append(uniqueIdentifier.getUniqueString());
        }
        LOG.debug("Persisting fragment of status [{}] for key [{}] uniqueIds [{}]", status.toString(),
          fragment.getKey() == null ? "null" : fragment.getKey().toString(), uniques.toString());
      }
    }

    final TimerContext context = persistenceTimer.time();
    try {
      if (status == OccurrencePersistenceStatus.NEW) {
        FragmentCreationResult creationResult = fragmentPersister.insert(fragment, uniqueIds);
        if (!creationResult.isKeyCreated()) {
          // we lost a race to generate the key for these uniqueIds, and now this is an update
          status = OccurrencePersistenceStatus.UPDATED;
          LOG.info(
            "Fragment creation did not generate new key - lost race and now using existing [{}] as status [UPDATE]",
            fragment.getKey());
        }
      } else {
        fragmentPersister.update(fragment);
      }
    } catch (ServiceUnavailableException e) {
      failForServiceUnavailable(datasetKey, e);
      return;
    } catch (IllegalDataStateException e) {
      failForDataStateException(datasetKey, e);
    } finally {
      context.stop();
    }

    ZookeeperConnector.CounterName counterToUpdate = null;
    switch (status) {
      case UNCHANGED:
        counterToUpdate = ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED;
        break;
      case NEW:
        counterToUpdate = ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW;
        break;
      case UPDATED:
        counterToUpdate = ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED;
        break;
    }
    updateZookeeper(fragment.getDatasetKey(), counterToUpdate);

    Message fragMsg = new FragmentPersistedMessage(fragment.getDatasetKey(), crawlId, status, fragment.getKey());
    final TimerContext msgContext = msgTimer.time();
    try {
      LOG.debug("Sending FragmentPersistedMessage for key [{}]", fragment.getKey());
      messagePublisher.send(fragMsg);
      msgsSent.mark();
    } catch (IOException e) {
      LOG.warn("Could not send FragmentPersistedMessage for successful persist of status [{}]", status, e);
    } finally {
      msgContext.stop();
    }

    fragmentsProcessed.mark();
  }

  private void failForServiceUnavailable(UUID datasetKey, Throwable e) {
    LOG.warn("Caught service unavailable from HBase: this fragment for dataset [{}] won't be processed", datasetKey, e);
    updateZookeeper(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
  }

  private void failForDataStateException(UUID datasetKey, Throwable e) {
    LOG.warn("Data inconsistency prevents this fragment being processed for dataset [{}]: {}", datasetKey,
             e.getMessage(), e);
    updateZookeeper(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
  }

  private void updateZookeeper(UUID datasetKey, ZookeeperConnector.CounterName counterName) {
    LOG.debug("Updating zookeeper for RawOccurrencePersisted");
    zookeeperConnector.addCounter(datasetKey, counterName);
  }
}
