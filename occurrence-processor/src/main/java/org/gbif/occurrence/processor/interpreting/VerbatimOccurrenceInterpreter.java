package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.common.InterpretedEnum;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrencePersistenceStatus;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.typestatus.InterpretedTypeStatusParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.result.DateInterpretationResult;
import org.gbif.occurrence.processor.interpreting.util.BasisOfRecordInterpreter;
import org.gbif.occurrence.processor.interpreting.util.DateInterpreter;
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

  private static final InterpretedTypeStatusParser typeParser = InterpretedTypeStatusParser.getInstance();

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

      interpretEventDate(verbatim, occ);

      interpretModifiedDate(verbatim, occ);

      interpretTypification(verbatim, occ);

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
      zookeeperConnector
        .addCounter(occ.getDatasetKey(), ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    }

    return result;
  }

  private void interpretTypification(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasField(DwcTerm.typeStatus)) {
      ParseResult<InterpretedEnum<String, TypeStatus>> parsed = typeParser.parse(verbatim.getField(DwcTerm.typeStatus));
      if (parsed.isSuccessful()) {
        occ.setTypeStatus(parsed.getPayload().getInterpreted());
      }
    }
  }

  private static void interpretModifiedDate(VerbatimOccurrence verbatim, Occurrence occ) {
    occ.setModified(DateInterpreter.interpretDate(verbatim.getField(DcTerm.modified), 1900));
  }


  private static void interpretEventDate(VerbatimOccurrence verbatim, Occurrence occ) {
    DateInterpretationResult dateResult = DateInterpreter.interpretRecordedDate(verbatim.getField(DwcTerm.year),
                                                                                verbatim.getField(DwcTerm.month),
                                                                                verbatim.getField(DwcTerm.day),
                                                                                verbatim.getField(DwcTerm.eventDate));
    occ.setEventDate(dateResult.getDate());
    occ.setMonth(dateResult.getMonth());
    occ.setYear(dateResult.getYear());
    occ.setDay(dateResult.getDay());
    // copy rules
    occ.getIssues().addAll(dateResult.getIssues());

    LOG.debug("Got recorded date [{}]: day [{}] month [{}] year [{}]",
      dateResult.getDate(), dateResult.getDay(), dateResult.getMonth(), dateResult.getYear());
  }

  private static void interpretBor(VerbatimOccurrence verbatim, Occurrence occ) {
    //TODO: log issues
    BasisOfRecord bor = BasisOfRecordInterpreter.interpretBasisOfRecord(verbatim.getField(DwcTerm.basisOfRecord)).getPayload();
    occ.setBasisOfRecord(bor);
    LOG.debug("Got BOR [{}]", bor);
  }



}
