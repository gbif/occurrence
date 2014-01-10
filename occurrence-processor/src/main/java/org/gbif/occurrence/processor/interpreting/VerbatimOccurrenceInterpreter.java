package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrencePersistenceStatus;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;
import org.gbif.occurrence.interpreters.AltitudeInterpreter;
import org.gbif.occurrence.interpreters.BasisOfRecordInterpreter;
import org.gbif.occurrence.interpreters.DateInterpreter;
import org.gbif.occurrence.interpreters.DepthInterpreter;
import org.gbif.occurrence.interpreters.result.DateInterpretationResult;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.api.VerbatimOccurrence;
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
   * @return an InterpretationResult that contains an "updated" Occurrence with interpreted fields and an "original"
   *         occurrence iff this was an update to an existing record (will be null otherwise)
   */
  public InterpretationResult interpret(VerbatimOccurrence verbatim, OccurrencePersistenceStatus status,
    boolean fromCrawl) {
    Occurrence occ = occurrenceFrom(verbatim);

    try {
      ExecutorService threadPool = Executors.newFixedThreadPool(3);
      Future<?> coordFuture = threadPool.submit(new CoordBasedInterpreter(verbatim, occ));
      Future<?> taxonomyFuture = threadPool.submit(new TaxonomyInterpreter(verbatim, occ));
      Future<?> hostCountryFuture = threadPool.submit(new OwningOrgInterpreter(occ));
      threadPool.shutdown();

      interpretAltitude(verbatim, occ);

      interpretBor(verbatim, occ);

      interpretDate(verbatim, occ);

      interpretDepth(verbatim, occ);

      // TODO: if any errors, update messages
      //   possible errors:
      //     GEO: lat/lng out of bounds, lat/lng reversed for given country, others?
      //     TAXONOMY: name not found, others?
      //     BASIS: basis not interpreted, basis not provided, others?
      //     TEMPORAL: dates uninterpretable (eg 10-11-12)
      //     ALT_DEPTH: any errors?

      occ.setModified(new Date());

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
    InterpretationResult result = new InterpretationResult(original, occ);

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

  private static void interpretDepth(VerbatimOccurrence verbatim, Occurrence occ) {
    Integer depth =
      DepthInterpreter.interpretDepth(verbatim.getMinDepth(), verbatim.getMaxDepth(), verbatim.getDepthPrecision());
    occ.setDepth(depth);
    LOG.debug("Got depth [{}]", depth);
  }

  private static void interpretDate(VerbatimOccurrence verbatim, Occurrence occ) {
    DateInterpretationResult dateResult = DateInterpreter
      .interpretDate(verbatim.getYear(), verbatim.getMonth(), verbatim.getDay(), verbatim.getOccurrenceDate());
    if (dateResult == null) {
      LOG.debug("Got date [null]");
    } else {
      occ.setEventDate(dateResult.getDate());
      occ.setMonth(dateResult.getMonth());
      occ.setYear(dateResult.getYear());
      LOG.debug("Got date [{}] month [{}] year [{}]",
        dateResult.getDate() == null ? "null" : dateResult.getDate().toString(),
        dateResult.getMonth() == null ? "null" : dateResult.getMonth().toString(),
        dateResult.getYear() == null ? "null" : dateResult.getYear().toString());
    }
  }

  private static void interpretAltitude(VerbatimOccurrence verbatim, Occurrence occ) {
    Integer alt = AltitudeInterpreter
      .interpretAltitude(verbatim.getMinAltitude(), verbatim.getMaxAltitude(), verbatim.getAltitudePrecision());
    occ.setAltitude(alt);
    LOG.debug("Got altitude [{}]", alt);
  }

  private static void interpretBor(VerbatimOccurrence verbatim, Occurrence occ) {
    // TODO: interpretation should produce enum
    BasisOfRecordConverter borConv = new BasisOfRecordConverter();
    Integer basisOfRecord = BasisOfRecordInterpreter.interpretBasisOfRecord(verbatim.getBasisOfRecord());
    BasisOfRecord bor = borConv.toEnum(basisOfRecord);
    occ.setBasisOfRecord(bor);
    LOG.debug("Got BOR [{}]", bor.toString());
  }

  private Occurrence occurrenceFrom(VerbatimOccurrence verbatim) {
    Occurrence occ = new Occurrence();
    occ.setKey(verbatim.getKey());
//    occ.setInstitutionCode(verbatim.getInstitutionCode());
//    occ.setCollectionCode(verbatim.getCollectionCode());
//    occ.setCatalogNumber(verbatim.getCatalogNumber());
//    occ.setUnitQualifier(verbatim.getUnitQualifier());
    occ.setDatasetKey(verbatim.getDatasetKey());
    occ.setProtocol(verbatim.getProtocol());

    // these are the as-yet uninterpreted fields
//    occ.setCollectorName(verbatim.getCollectorName());
//    occ.setContinent(verbatim.getContinentOrOcean());
//    occ.setCounty(verbatim.getCounty());
//    occ.setDataProviderId(verbatim.getDataProviderId());
//    occ.setDataResourceId(verbatim.getDataResourceId());
//    occ.setIdentifierName(verbatim.getIdentifierName());
//    occ.setLocality(verbatim.getLocality());
//    occ.setOwningOrgKey(verbatim.getOwningOrgKey());
//    occ.setResourceAccessPointId(verbatim.getResourceAccessPointId());
    occ.setStateProvince(verbatim.getStateOrProvince());

    return occ;
  }
}
