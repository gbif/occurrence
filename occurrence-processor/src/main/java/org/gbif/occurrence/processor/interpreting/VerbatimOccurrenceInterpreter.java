package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.EstablishmentMeansParser;
import org.gbif.common.parsers.LifeStageParser;
import org.gbif.common.parsers.SexParser;
import org.gbif.common.parsers.TypeStatusParser;
import org.gbif.common.parsers.TypifiedNameParser;
import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

  private static final TypeStatusParser TYPE_PARSER = TypeStatusParser.getInstance();
  private static final Parsable<String> TYPE_NAME_PARSER = TypifiedNameParser.getInstance();
  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final SexParser SEX_PARSER = SexParser.getInstance();
  private static final EstablishmentMeansParser EST_PARSER = EstablishmentMeansParser.getInstance();
  private static final LifeStageParser LST_PARSER = LifeStageParser.getInstance();

  private final OccurrencePersistenceService occurrenceService;
  private final ZookeeperConnector zookeeperConnector;

  @Inject
  public VerbatimOccurrenceInterpreter(OccurrencePersistenceService occurrenceService,
    ZookeeperConnector zookeeperConnector) {
    this.occurrenceService = checkNotNull(occurrenceService, "occurrenceService can't be null");
    this.zookeeperConnector = checkNotNull(zookeeperConnector, "zookeeperConnector can't be null");
  }

  /**
   * Interpret all the verbatim fields into our standard Occurrence fields.
   * TODO: send messages/write logs for interpretation errors
   *
   * @param verbatim the verbatim occurrence to interpret
   *
   * @return an OccurrenceInterpretationResult that contains an "updated" Occurrence with interpreted fields and an "original"
   *         occurrence iff this was an update to an existing record (will be null otherwise)
   */
  public OccurrenceInterpretationResult interpret(VerbatimOccurrence verbatim, OccurrencePersistenceStatus status,
    boolean fromCrawl) {
    Occurrence occ = new Occurrence(verbatim);

    try {
      ExecutorService threadPool = Executors.newFixedThreadPool(3);
      Future<?> coordFuture = threadPool.submit(new LocationInterpreter(verbatim, occ));
      Future<?> taxonomyFuture = threadPool.submit(new TaxonomyInterpreter(verbatim, occ));
      Future<?> hostCountryFuture = threadPool.submit(new OwningOrgInterpreter(occ));
      threadPool.shutdown();

      interpretBor(verbatim, occ);
      interpretSex(verbatim, occ);
      interpretEstablishmentMeans(verbatim, occ);
      interpretLifeStage(verbatim, occ);
      interpretTypification(verbatim, occ);
      TemporalInterpreter.interpretTemporal(verbatim, occ);

      occ.setLastInterpreted(new Date());

      // wait for the ws calls
      if (!threadPool.awaitTermination(2, TimeUnit.MINUTES)) {
        if (coordFuture.isCancelled()) {
          LOG.warn("Coordinate lookup timed out - geospatial will be wrong for occurrence [{}]", occ.getKey());
        }
        if (taxonomyFuture.isCancelled()) {
          LOG.warn("Nub lookup timed out - taxonomy will be wrong for occurrence [{}]", occ.getKey());
        }
        if (hostCountryFuture.isCancelled()) {
          LOG.warn("Host country lookup timed out - host country will be null for occurrence [{}]", occ.getKey());
        }
      }

    } catch (Exception e) {
      // TODO: tidy up exception handling to not be Exception
      LOG.warn("Caught a runtime exception during interpretation", e);
    }

    // populate the returned class
    Occurrence original = null;
    if (status == OccurrencePersistenceStatus.UPDATED) {
      original = occurrenceService.get(verbatim.getKey());
      // TODO: compare original with interp - if identical then return unchanged flag on result, so downstream
      // doesn't need to do anything
    }
    OccurrenceInterpretationResult result = new OccurrenceInterpretationResult(original, occ);

    // persist the record (considered an update in all cases because the key must already exist on verbatim)
    LOG.debug("Persisting interpreted occurrence");
    occurrenceService.update(occ);

    if (fromCrawl) {
      LOG.debug("Updating zookeeper for OccurrenceInterpretedPersistedSuccess");
      zookeeperConnector.addCounter(occ.getDatasetKey(),
                                    ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    }

    return result;
  }

  private void interpretTypification(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.typeStatus)) {
      ParseResult<TypeStatus> parsed = TYPE_PARSER.parse(verbatim.getVerbatimField(DwcTerm.typeStatus));
      occ.setTypeStatus(parsed.getPayload());
      occ.getIssues().addAll(parsed.getIssues());

      ParseResult<String> parsedName = TYPE_NAME_PARSER.parse(verbatim.getVerbatimField(DwcTerm.typeStatus));
      occ.setTypifiedName(parsedName.getPayload());
      occ.getIssues().addAll(parsedName.getIssues());
    }
  }

  private static void interpretBor(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<BasisOfRecord> parsed = BOR_PARSER.parse(verbatim.getVerbatimField(DwcTerm.basisOfRecord));
    if (parsed.isSuccessful()) {
      occ.setBasisOfRecord(parsed.getPayload());
    } else {
      LOG.debug("Unknown basisOfRecord [{}]", verbatim.getVerbatimField(DwcTerm.basisOfRecord));
    }
  }

  private static void interpretSex(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<Sex> parsed = SEX_PARSER.parse(verbatim.getVerbatimField(DwcTerm.sex));
    if (parsed.isSuccessful()) {
      occ.setSex(parsed.getPayload());
    } else {
      LOG.debug("Unknown sex [{}]", verbatim.getVerbatimField(DwcTerm.sex));
    }
  }

  private static void interpretEstablishmentMeans(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<EstablishmentMeans> parsed = EST_PARSER.parse(verbatim.getVerbatimField(DwcTerm.establishmentMeans));
    if (parsed.isSuccessful()) {
      occ.setEstablishmentMeans(parsed.getPayload());
    } else {
      LOG.debug("Unknown establishmentMeans [{}]", verbatim.getVerbatimField(DwcTerm.establishmentMeans));
    }
  }

  private static void interpretLifeStage(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<LifeStage> parsed = LST_PARSER.parse(verbatim.getVerbatimField(DwcTerm.lifeStage));
    if (parsed.isSuccessful()) {
      occ.setLifeStage(parsed.getPayload());
    } else {
      LOG.debug("Unknown lifeStage [{}]", verbatim.getVerbatimField(DwcTerm.lifeStage));
    }
  }
}
