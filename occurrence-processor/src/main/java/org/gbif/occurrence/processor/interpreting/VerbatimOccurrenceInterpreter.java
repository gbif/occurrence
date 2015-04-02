package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
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
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Date;

import com.google.common.base.Strings;
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

  private final PublishingOrgInterpreter publishingOrgInterpreter;
  private final TaxonomyInterpreter taxonomyInterpreter;
  private final LocationInterpreter locationInterpreter;
  private final OccurrencePersistenceService occurrenceService;
  private final ZookeeperConnector zookeeperConnector;

  @Inject
  public VerbatimOccurrenceInterpreter(OccurrencePersistenceService occurrenceService,
    ZookeeperConnector zookeeperConnector, PublishingOrgInterpreter publishingOrgInterpreter,
    TaxonomyInterpreter taxonomyInterpreter, LocationInterpreter locationInterpreter) {
    this.publishingOrgInterpreter = publishingOrgInterpreter;
    this.taxonomyInterpreter = taxonomyInterpreter;
    this.locationInterpreter = locationInterpreter;
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
    Occurrence occ = new Occurrence(verbatim);

    // TODO: these interpreters throw a variety of runtime exceptions but should throw checked exceptions
    try {
      locationInterpreter.interpretLocation(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during location interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      taxonomyInterpreter.interpretTaxonomy(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during taxonomy interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      publishingOrgInterpreter.interpretPublishingOrg(occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during owning org interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      MultiMediaInterpreter.interpretMedia(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during media interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      interpretBor(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during basis of record interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      interpretSex(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during sex interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      interpretEstablishmentMeans(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during establishment means interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      interpretLifeStage(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during life stage interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      interpretTypification(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during typification interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      TemporalInterpreter.interpretTemporal(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during temporal interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }
    try {
      interpretReferences(verbatim, occ);
    } catch (Exception e) {
      LOG.warn("Caught a runtime exception during basis of record interpretation", e);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
    }

    occ.setLastInterpreted(new Date());

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

  private static void interpretReferences(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DcTerm.references)) {
      String val = verbatim.getVerbatimField(DcTerm.references);
      if (!Strings.isNullOrEmpty(val)) {
        occ.setReferences(UrlParser.parse(val));
        if (occ.getReferences() == null) {
          occ.getIssues().add(OccurrenceIssue.REFERENCES_URI_INVALID);
        }
      }
    }
  }

  private static void interpretTypification(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.typeStatus)) {
      ParseResult<TypeStatus> parsed = TYPE_PARSER.parse(verbatim.getVerbatimField(DwcTerm.typeStatus));
      occ.setTypeStatus(parsed.getPayload());

      ParseResult<String> parsedName = TYPE_NAME_PARSER.parse(verbatim.getVerbatimField(DwcTerm.typeStatus));
      occ.setTypifiedName(parsedName.getPayload());
    }
    if (verbatim.hasVerbatimField(GbifTerm.typifiedName)) {
      occ.setTypifiedName(verbatim.getVerbatimField(GbifTerm.typifiedName));
    }
  }

  private static void interpretBor(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<BasisOfRecord> parsed = BOR_PARSER.parse(verbatim.getVerbatimField(DwcTerm.basisOfRecord));
    if (parsed.isSuccessful()) {
      occ.setBasisOfRecord(parsed.getPayload());
    } else {
      LOG.debug("Unknown basisOfRecord [{}]", verbatim.getVerbatimField(DwcTerm.basisOfRecord));
      occ.setBasisOfRecord(BasisOfRecord.UNKNOWN);
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
