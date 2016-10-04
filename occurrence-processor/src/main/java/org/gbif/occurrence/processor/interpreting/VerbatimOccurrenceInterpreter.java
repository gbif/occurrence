package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Produces Occurrence objects by interpreting the verbatim fields of a VerbatimOccurrence.
 */
@Singleton
public class VerbatimOccurrenceInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(VerbatimOccurrenceInterpreter.class);

  private final OccurrenceInterpreter occurrenceInterpreter;
  private final OccurrencePersistenceService occurrenceService;
  private final ZookeeperConnector zookeeperConnector;

  @Inject
  public VerbatimOccurrenceInterpreter(OccurrencePersistenceService occurrenceService,
    ZookeeperConnector zookeeperConnector, OccurrenceInterpreter occurrenceInterpreter) {
    this.occurrenceInterpreter = occurrenceInterpreter;
    this.occurrenceService = checkNotNull(occurrenceService, "occurrenceService can't be null");
    this.zookeeperConnector = checkNotNull(zookeeperConnector, "zookeeperConnector can't be null");
  }

  /**
   * Interpret all the verbatim fields into our standard Occurrence fields.
   * TODO: send messages/write logs for interpretation errors
   *
   * @param verbatim the verbatim occurrence to interpret
   *
   * @return an OccurrenceInterpretationResult that contains an "updated" Occurrence with interpreted fields and an
   * "original"
   * occurrence iff this was an update to an existing record (will be null otherwise)
   */
  public OccurrenceInterpretationResult interpret(VerbatimOccurrence verbatim, OccurrencePersistenceStatus status,
    boolean fromCrawl) {
    OccurrenceInterpretationResult result = occurrenceInterpreter.interpret(verbatim);
    // persist the record (considered an update in all cases because the key must already exist on verbatim)
    LOG.debug("Persisting interpreted occurrence");
    occurrenceService.update(result.getUpdated());

    if (fromCrawl) {
      LOG.debug("Updating zookeeper for OccurrenceInterpretedPersistedSuccess");
      zookeeperConnector
        .addCounter(result.getUpdated().getDatasetKey(), ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    }

    return result;
  }

}
