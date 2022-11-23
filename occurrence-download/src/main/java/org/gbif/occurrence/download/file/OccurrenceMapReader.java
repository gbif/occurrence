/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.file;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.AgentIdentifier;
import org.gbif.api.model.occurrence.GadmFeature;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.net.URI;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static org.gbif.occurrence.common.download.DownloadUtils.DELIMETERS_MATCH_PATTERN;

/**
 * Reads a occurrence record from Elasticsearch and return it in a Map<String,Object>.
 */
public class OccurrenceMapReader {

  private OccurrenceMapReader() {
    // NOP
  }

  public static final Map<Rank, Term> rank2KeyTerm =
    ImmutableMap.<Rank, Term>builder().put(Rank.KINGDOM, GbifTerm.kingdomKey).put(Rank.PHYLUM, GbifTerm.phylumKey)
      .put(Rank.CLASS, GbifTerm.classKey).put(Rank.ORDER, GbifTerm.orderKey).put(Rank.FAMILY, GbifTerm.familyKey)
      .put(Rank.GENUS, GbifTerm.genusKey).put(Rank.SUBGENUS, GbifTerm.subgenusKey)
      .put(Rank.SPECIES, GbifTerm.speciesKey).build();

  public static final Map<Rank, Term> rank2Term =
    ImmutableMap.<Rank, Term>builder().put(Rank.KINGDOM, DwcTerm.kingdom).put(Rank.PHYLUM, DwcTerm.phylum)
      .put(Rank.CLASS, DwcTerm.class_).put(Rank.ORDER, DwcTerm.order).put(Rank.FAMILY, DwcTerm.family)
      .put(Rank.GENUS, DwcTerm.genus).put(Rank.SUBGENUS, DwcTerm.subgenus)
      .put(Rank.SPECIES, GbifTerm.species).build();

  private static final ImmutableSet<Term> INTERPRETED_SOURCE_TERMS = ImmutableSet.copyOf(TermUtils.interpretedSourceTerms());


  public static Map<String, String> buildInterpretedOccurrenceMap(Occurrence occurrence) {

    Map<String,String> interpretedOccurrence = new HashMap<>();

    //Basic record terms
    interpretedOccurrence.put(GbifTerm.gbifID.simpleName(), getSimpleValue(occurrence.getKey()));
    interpretedOccurrence.put(DwcTerm.basisOfRecord.simpleName(), getSimpleValue(occurrence.getBasisOfRecord()));
    interpretedOccurrence.put(DwcTerm.establishmentMeans.simpleName(), getSimpleValue(occurrence.getEstablishmentMeans()));
    interpretedOccurrence.put(DwcTerm.individualCount.simpleName(), getSimpleValue(occurrence.getIndividualCount()));
    interpretedOccurrence.put(DwcTerm.lifeStage.simpleName(), getSimpleValue(occurrence.getLifeStage()));
    interpretedOccurrence.put(DwcTerm.pathway.simpleName(), getSimpleValue(occurrence.getPathway()));
    interpretedOccurrence.put(DwcTerm.degreeOfEstablishment.simpleName(), getSimpleValue(occurrence.getDegreeOfEstablishment()));
    interpretedOccurrence.put(DcTerm.references.simpleName(), getSimpleValue(occurrence.getReferences()));
    interpretedOccurrence.put(DwcTerm.sex.simpleName(), getSimpleValue(occurrence.getSex()));
    interpretedOccurrence.put(DwcTerm.typeStatus.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getTypeStatus()));
    interpretedOccurrence.put(GbifTerm.typifiedName.simpleName(), occurrence.getTypifiedName());
    interpretedOccurrence.put(GbifTerm.lastParsed.simpleName(), getSimpleValue(occurrence.getLastParsed()));
    interpretedOccurrence.put(GbifTerm.lastInterpreted.simpleName(), getSimpleValue(occurrence.getLastInterpreted()));
    interpretedOccurrence.put(DwcTerm.occurrenceStatus.simpleName(), getSimpleValue(occurrence.getOccurrenceStatus()));
    interpretedOccurrence.put(DwcTerm.datasetID.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getDatasetID()));
    interpretedOccurrence.put(DwcTerm.datasetName.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getDatasetName()));
    interpretedOccurrence.put(DwcTerm.otherCatalogNumbers.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getOtherCatalogNumbers()));
    interpretedOccurrence.put(DwcTerm.recordedBy.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getRecordedBy()));
    interpretedOccurrence.put(DwcTerm.identifiedBy.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getIdentifiedBy()));
    interpretedOccurrence.put(DwcTerm.preparations.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getPreparations()));
    interpretedOccurrence.put(DwcTerm.samplingProtocol.simpleName(), getSimpleValueAndNormalizeDelimiters(occurrence.getSamplingProtocol()));

    Optional.ofNullable(occurrence.getVerbatimField(DcTerm.identifier))
      .ifPresent(x -> interpretedOccurrence.put(DcTerm.identifier.simpleName(), x));

    //Dataset Metadata
    interpretedOccurrence.put(GbifInternalTerm.crawlId.simpleName(), getSimpleValue(occurrence.getCrawlId()));
    interpretedOccurrence.put(GbifTerm.datasetKey.simpleName(), getSimpleValue(occurrence.getDatasetKey()));
    interpretedOccurrence.put(GbifTerm.publishingCountry.simpleName(), getCountryCode(occurrence.getPublishingCountry()));
    interpretedOccurrence.put(GbifInternalTerm.installationKey.simpleName(), getSimpleValue(occurrence.getInstallationKey()));
    interpretedOccurrence.put(DcTerm.license.simpleName(), getSimpleValue(occurrence.getLicense()));
    interpretedOccurrence.put(GbifTerm.protocol.simpleName(), getSimpleValue(occurrence.getProtocol()));
    interpretedOccurrence.put(GbifInternalTerm.networkKey.simpleName(), joinUUIDs(occurrence.getNetworkKeys()));
    interpretedOccurrence.put(GbifInternalTerm.publishingOrgKey.simpleName(), getSimpleValue(occurrence.getPublishingOrgKey()));
    interpretedOccurrence.put(GbifTerm.lastCrawled.simpleName(), getSimpleValue(occurrence.getLastCrawled()));

    //Temporal fields
    interpretedOccurrence.put(DwcTerm.dateIdentified.simpleName(), getLocalDateValue(occurrence.getDateIdentified()));
    interpretedOccurrence.put(DcTerm.modified.simpleName(),getSimpleValue(occurrence.getModified()));
    interpretedOccurrence.put(DwcTerm.day.simpleName(), getSimpleValue(occurrence.getDay()));
    interpretedOccurrence.put(DwcTerm.month.simpleName(), getSimpleValue(occurrence.getMonth()));
    interpretedOccurrence.put(DwcTerm.year.simpleName(), getSimpleValue(occurrence.getYear()));
    interpretedOccurrence.put(DwcTerm.eventDate.simpleName(), getLocalDateValue(occurrence.getEventDate()));

    // taxonomy terms
    interpretedOccurrence.put(GbifTerm.taxonKey.simpleName(), getSimpleValue(occurrence.getTaxonKey()));
    interpretedOccurrence.put(GbifTerm.acceptedTaxonKey.simpleName(), getSimpleValue(occurrence.getAcceptedTaxonKey()));
    interpretedOccurrence.put(DwcTerm.scientificName.simpleName(), occurrence.getScientificName());
    interpretedOccurrence.put(GbifTerm.acceptedScientificName.simpleName(), occurrence.getAcceptedScientificName());
    interpretedOccurrence.put(GbifTerm.verbatimScientificName.simpleName(), occurrence.getVerbatimScientificName());
    interpretedOccurrence.put(DwcTerm.genericName.simpleName(), occurrence.getGenericName());
    interpretedOccurrence.put(GbifTerm.subgenusKey.simpleName(), getSimpleValue(occurrence.getSubgenusKey()));
    interpretedOccurrence.put(DwcTerm.specificEpithet.simpleName(), occurrence.getSpecificEpithet());
    interpretedOccurrence.put(DwcTerm.infraspecificEpithet.simpleName(), occurrence.getInfraspecificEpithet());
    interpretedOccurrence.put(DwcTerm.taxonRank.simpleName(), getSimpleValue(occurrence.getTaxonRank()));
    interpretedOccurrence.put(DwcTerm.taxonomicStatus.simpleName(), getSimpleValue(occurrence.getTaxonomicStatus()));
    interpretedOccurrence.put(DwcTerm.genericName.simpleName(), getSimpleValue(occurrence.getGenericName()));
    Rank.DWC_RANKS.forEach(rank -> {
                              Optional.ofNullable(ClassificationUtils.getHigherRankKey(occurrence, rank))
                                .ifPresent(rankKey -> interpretedOccurrence.put(rank2KeyTerm.get(rank).simpleName(), rankKey.toString()));
                              Optional.ofNullable(ClassificationUtils.getHigherRank(occurrence, rank))
                                .ifPresent(rankClassification -> interpretedOccurrence.put(rank2Term.get(rank).simpleName(), rankClassification));
                           });
    interpretedOccurrence.put(IucnTerm.iucnRedListCategory.simpleName(), getSimpleValue(occurrence.getIucnRedListCategory()));

    //location fields
    interpretedOccurrence.put(DwcTerm.countryCode.simpleName(), getCountryCode(occurrence.getCountry()));
    interpretedOccurrence.put(DwcTerm.continent.simpleName(), getSimpleValue(occurrence.getContinent()));
    interpretedOccurrence.put(DwcTerm.decimalLatitude.simpleName(), getSimpleValue(occurrence.getDecimalLatitude()));
    interpretedOccurrence.put(DwcTerm.decimalLongitude.simpleName(), getSimpleValue(occurrence.getDecimalLongitude()));
    interpretedOccurrence.put(DwcTerm.coordinatePrecision.simpleName(), getSimpleValue(occurrence.getCoordinatePrecision()));
    interpretedOccurrence.put(DwcTerm.coordinateUncertaintyInMeters.simpleName(), getSimpleValue(occurrence.getCoordinateUncertaintyInMeters()));
    interpretedOccurrence.put(GbifTerm.depth.simpleName(), getSimpleValue(occurrence.getDepth()));
    interpretedOccurrence.put(GbifTerm.depthAccuracy.simpleName(), getSimpleValue(occurrence.getDepthAccuracy()));
    interpretedOccurrence.put(GbifTerm.elevation.simpleName(), getSimpleValue(occurrence.getElevation()));
    interpretedOccurrence.put(GbifTerm.elevationAccuracy.simpleName(), getSimpleValue(occurrence.getElevationAccuracy()));
    interpretedOccurrence.put(DwcTerm.stateProvince.simpleName(), occurrence.getStateProvince());
    interpretedOccurrence.put(DwcTerm.waterBody.simpleName(), occurrence.getWaterBody());
    interpretedOccurrence.put(GbifTerm.hasGeospatialIssues.simpleName(), Boolean.toString(occurrence.hasSpatialIssue()));
    interpretedOccurrence.put(GbifTerm.hasCoordinate.simpleName(), Boolean.toString(occurrence.getDecimalLatitude() != null && occurrence.getDecimalLongitude() != null));
    interpretedOccurrence.put(GbifTerm.coordinateAccuracy.simpleName(), getSimpleValue(occurrence.getCoordinateAccuracy()));
    getRepatriated(occurrence).ifPresent(repatriated -> interpretedOccurrence.put(GbifTerm.repatriated.simpleName(), repatriated));
    interpretedOccurrence.put(DwcTerm.geodeticDatum.simpleName(), occurrence.getGeodeticDatum());
    putGadmFeature(interpretedOccurrence, GadmTerm.level0Name, GadmTerm.level0Gid, occurrence.getGadm().getLevel0());
    putGadmFeature(interpretedOccurrence, GadmTerm.level1Name, GadmTerm.level1Gid, occurrence.getGadm().getLevel1());
    putGadmFeature(interpretedOccurrence, GadmTerm.level2Name, GadmTerm.level2Gid, occurrence.getGadm().getLevel2());
    putGadmFeature(interpretedOccurrence, GadmTerm.level3Name, GadmTerm.level3Gid, occurrence.getGadm().getLevel3());

    extractOccurrenceIssues(occurrence.getIssues())
      .ifPresent(issues -> interpretedOccurrence.put(GbifTerm.issue.simpleName(), issues));
    extractMediaTypes(occurrence.getMedia())
      .ifPresent(mediaTypes -> interpretedOccurrence.put(GbifTerm.mediaType.simpleName(), mediaTypes));
    extractAgentIds(occurrence.getRecordedByIds())
      .ifPresent(uids -> interpretedOccurrence.put(DwcTerm.recordedByID.simpleName(), uids));
    extractAgentIds(occurrence.getIdentifiedByIds())
      .ifPresent(uids -> interpretedOccurrence.put(DwcTerm.identifiedByID.simpleName(), uids));

    // Sampling
    interpretedOccurrence.put(DwcTerm.sampleSizeUnit.simpleName(), occurrence.getSampleSizeUnit());
    interpretedOccurrence.put(DwcTerm.sampleSizeValue.simpleName(), getSimpleValue(occurrence.getSampleSizeValue()));
    interpretedOccurrence.put(DwcTerm.organismQuantity.simpleName(), getSimpleValue(occurrence.getOrganismQuantity()));
    interpretedOccurrence.put(DwcTerm.organismQuantityType.simpleName(), occurrence.getOrganismQuantityType());
    interpretedOccurrence.put(GbifTerm.relativeOrganismQuantity.simpleName(), getSimpleValue(occurrence.getRelativeOrganismQuantity()));

    occurrence.getVerbatimFields().forEach( (term, value) -> {
      if (!INTERPRETED_SOURCE_TERMS.contains(term)) {
       interpretedOccurrence.put(term.simpleName(), value);
      }
    });

    return interpretedOccurrence;
  }

  public static Map<String, String> buildInterpretedEventMap(Event event) {

    Map<String,String> interpretedEvent = new HashMap<>();

    //Basic record terms
    interpretedEvent.put(GbifTerm.gbifID.simpleName(), getSimpleValue(event.getKey()));
    interpretedEvent.put(DwcTerm.basisOfRecord.simpleName(), getSimpleValue(event.getBasisOfRecord()));
    interpretedEvent.put(DwcTerm.establishmentMeans.simpleName(), getSimpleValue(event.getEstablishmentMeans()));
    interpretedEvent.put(DwcTerm.individualCount.simpleName(), getSimpleValue(event.getIndividualCount()));
    interpretedEvent.put(DwcTerm.lifeStage.simpleName(), getSimpleValue(event.getLifeStage()));
    interpretedEvent.put(DwcTerm.pathway.simpleName(), getSimpleValue(event.getPathway()));
    interpretedEvent.put(DwcTerm.degreeOfEstablishment.simpleName(), getSimpleValue(event.getDegreeOfEstablishment()));
    interpretedEvent.put(DcTerm.references.simpleName(), getSimpleValue(event.getReferences()));
    interpretedEvent.put(DwcTerm.sex.simpleName(), getSimpleValue(event.getSex()));
    interpretedEvent.put(GbifTerm.lastParsed.simpleName(), getSimpleValue(event.getLastParsed()));
    interpretedEvent.put(GbifTerm.lastInterpreted.simpleName(), getSimpleValue(event.getLastInterpreted()));
    interpretedEvent.put(DwcTerm.occurrenceStatus.simpleName(), getSimpleValue(event.getOccurrenceStatus()));
    interpretedEvent.put(DwcTerm.datasetID.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getDatasetID()));
    interpretedEvent.put(DwcTerm.datasetName.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getDatasetName()));
    interpretedEvent.put(DwcTerm.otherCatalogNumbers.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getOtherCatalogNumbers()));
    interpretedEvent.put(DwcTerm.recordedBy.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getRecordedBy()));
    interpretedEvent.put(DwcTerm.identifiedBy.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getIdentifiedBy()));
    interpretedEvent.put(DwcTerm.preparations.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getPreparations()));
    interpretedEvent.put(DwcTerm.samplingProtocol.simpleName(), getSimpleValueAndNormalizeDelimiters(event.getSamplingProtocol()));

    Optional.ofNullable(event.getVerbatimField(DcTerm.identifier))
      .ifPresent(x -> interpretedEvent.put(DcTerm.identifier.simpleName(), x));

    //Dataset Metadata
    interpretedEvent.put(GbifInternalTerm.crawlId.simpleName(), getSimpleValue(event.getCrawlId()));
    interpretedEvent.put(GbifTerm.datasetKey.simpleName(), getSimpleValue(event.getDatasetKey()));
    interpretedEvent.put(GbifTerm.publishingCountry.simpleName(), getCountryCode(event.getPublishingCountry()));
    interpretedEvent.put(GbifInternalTerm.installationKey.simpleName(), getSimpleValue(event.getInstallationKey()));
    interpretedEvent.put(DcTerm.license.simpleName(), getSimpleValue(event.getLicense()));
    interpretedEvent.put(GbifTerm.protocol.simpleName(), getSimpleValue(event.getProtocol()));
    interpretedEvent.put(GbifInternalTerm.networkKey.simpleName(), joinUUIDs(event.getNetworkKeys()));
    interpretedEvent.put(GbifInternalTerm.publishingOrgKey.simpleName(), getSimpleValue(event.getPublishingOrgKey()));
    interpretedEvent.put(GbifTerm.lastCrawled.simpleName(), getSimpleValue(event.getLastCrawled()));

    //Temporal fields
    interpretedEvent.put(DwcTerm.dateIdentified.simpleName(), getLocalDateValue(event.getDateIdentified()));
    interpretedEvent.put(DcTerm.modified.simpleName(),getSimpleValue(event.getModified()));
    interpretedEvent.put(DwcTerm.day.simpleName(), getSimpleValue(event.getDay()));
    interpretedEvent.put(DwcTerm.month.simpleName(), getSimpleValue(event.getMonth()));
    interpretedEvent.put(DwcTerm.year.simpleName(), getSimpleValue(event.getYear()));
    interpretedEvent.put(DwcTerm.eventDate.simpleName(), getLocalDateValue(event.getEventDate()));

    //location fields
    interpretedEvent.put(DwcTerm.countryCode.simpleName(), getCountryCode(event.getCountry()));
    interpretedEvent.put(DwcTerm.continent.simpleName(), getSimpleValue(event.getContinent()));
    interpretedEvent.put(DwcTerm.decimalLatitude.simpleName(), getSimpleValue(event.getDecimalLatitude()));
    interpretedEvent.put(DwcTerm.decimalLongitude.simpleName(), getSimpleValue(event.getDecimalLongitude()));
    interpretedEvent.put(DwcTerm.coordinatePrecision.simpleName(), getSimpleValue(event.getCoordinatePrecision()));
    interpretedEvent.put(DwcTerm.coordinateUncertaintyInMeters.simpleName(), getSimpleValue(event.getCoordinateUncertaintyInMeters()));
    interpretedEvent.put(GbifTerm.depth.simpleName(), getSimpleValue(event.getDepth()));
    interpretedEvent.put(GbifTerm.depthAccuracy.simpleName(), getSimpleValue(event.getDepthAccuracy()));
    interpretedEvent.put(GbifTerm.elevation.simpleName(), getSimpleValue(event.getElevation()));
    interpretedEvent.put(GbifTerm.elevationAccuracy.simpleName(), getSimpleValue(event.getElevationAccuracy()));
    interpretedEvent.put(DwcTerm.stateProvince.simpleName(), event.getStateProvince());
    interpretedEvent.put(DwcTerm.waterBody.simpleName(), event.getWaterBody());
    interpretedEvent.put(GbifTerm.hasGeospatialIssues.simpleName(), Boolean.toString(event.hasSpatialIssue()));
    interpretedEvent.put(GbifTerm.hasCoordinate.simpleName(), Boolean.toString(event.getDecimalLatitude() != null && event.getDecimalLongitude() != null));
    interpretedEvent.put(GbifTerm.coordinateAccuracy.simpleName(), getSimpleValue(event.getCoordinateAccuracy()));
    getRepatriated(event).ifPresent(repatriated -> interpretedEvent.put(GbifTerm.repatriated.simpleName(), repatriated));
    interpretedEvent.put(DwcTerm.geodeticDatum.simpleName(), event.getGeodeticDatum());
    putGadmFeature(interpretedEvent, GadmTerm.level0Name, GadmTerm.level0Gid, event.getGadm().getLevel0());
    putGadmFeature(interpretedEvent, GadmTerm.level1Name, GadmTerm.level1Gid, event.getGadm().getLevel1());
    putGadmFeature(interpretedEvent, GadmTerm.level2Name, GadmTerm.level2Gid, event.getGadm().getLevel2());
    putGadmFeature(interpretedEvent, GadmTerm.level3Name, GadmTerm.level3Gid, event.getGadm().getLevel3());

    extractOccurrenceIssues(event.getIssues())
      .ifPresent(issues -> interpretedEvent.put(GbifTerm.issue.simpleName(), issues));
    extractMediaTypes(event.getMedia())
      .ifPresent(mediaTypes -> interpretedEvent.put(GbifTerm.mediaType.simpleName(), mediaTypes));
    extractAgentIds(event.getRecordedByIds())
      .ifPresent(uids -> interpretedEvent.put(DwcTerm.recordedByID.simpleName(), uids));
    extractAgentIds(event.getIdentifiedByIds())
      .ifPresent(uids -> interpretedEvent.put(DwcTerm.identifiedByID.simpleName(), uids));

    // Sampling
    interpretedEvent.put(DwcTerm.sampleSizeUnit.simpleName(), event.getSampleSizeUnit());
    interpretedEvent.put(DwcTerm.sampleSizeValue.simpleName(), getSimpleValue(event.getSampleSizeValue()));
    interpretedEvent.put(DwcTerm.organismQuantity.simpleName(), getSimpleValue(event.getOrganismQuantity()));
    interpretedEvent.put(DwcTerm.organismQuantityType.simpleName(), event.getOrganismQuantityType());
    interpretedEvent.put(GbifTerm.relativeOrganismQuantity.simpleName(), getSimpleValue(event.getRelativeOrganismQuantity()));

    interpretedEvent.put(GbifTerm.gbifID.simpleName(), event.getId());
    interpretedEvent.put(DwcTerm.locationID.simpleName(), event.getLocationID());
    interpretedEvent.put(DwcTerm.parentEventID.simpleName(), event.getParentEventID());
    interpretedEvent.put(DwcTerm.startDayOfYear.simpleName(), getSimpleValue(event.getStartDayOfYear()));
    interpretedEvent.put(DwcTerm.endDayOfYear.simpleName(), getSimpleValue(event.getEndDayOfYear()));
    interpretedEvent.put(GbifTerm.eventType.simpleName(), getConcept(event.getEventType()));

    event.getVerbatimFields().forEach( (term, value) -> {
      if (!INTERPRETED_SOURCE_TERMS.contains(term)) {
        interpretedEvent.put(term.simpleName(), value);
      }
    });

    return interpretedEvent;
  }

  /**
   * Populate two verbatim fields for CSV downloads
   */
  public static void populateVerbatimCsvFields(Map<String, String> map, Occurrence occurrence) {
    Function<Term, String> keyFn =
      t -> "verbatim" + Character.toUpperCase(t.simpleName().charAt(0)) + t.simpleName().substring(1);

    Map<Term, String> verbatimFields = occurrence.getVerbatimFields();

    Optional.ofNullable(verbatimFields.get(DwcTerm.scientificName))
      .ifPresent(x -> map.put(keyFn.apply(DwcTerm.scientificName), x));
    Optional.ofNullable(verbatimFields.get(DwcTerm.scientificNameAuthorship))
      .ifPresent(x -> map.put(keyFn.apply(DwcTerm.scientificNameAuthorship), x));
  }

  /**
   * Builds Map that contains a lists of terms.
   */
  public static Map<String, String> buildInterpretedOccurrenceMap(Occurrence occurrence, Collection<Pair<DownloadTerms.Group, Term>> terms) {
    return  selectTerms(buildInterpretedOccurrenceMap(occurrence), terms);
  }

  /**
   * Builds Map that contains a lists of terms.
   */
  public static Map<String, String> selectTerms(Map<String,String> record, Collection<Pair<DownloadTerms.Group, Term>> terms) {
    return  record.entrySet().stream()
      .filter(entry -> terms.stream().anyMatch(term -> term.getRight().simpleName().equals(entry.getKey())))
      .collect(HashMap::new, (m,v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
  }

  /**
   * Joins a collection of UUIDs into String.
   */
  private static String joinUUIDs(Collection<UUID> uuids) {
    if (uuids != null ) {
     return uuids.stream().map(UUID::toString).collect(Collectors.joining(";"));
    }
    return null;
  }

  /**
   * Extract the Iso2LetterCode from the country.
   */
  private static String getCountryCode(Country country) {
    if (country != null) {
      return country.getIso2LetterCode();
    }
    return null;
  }

  /**
   * Transform a simple data type into a String.
   */
  private static String getSimpleValue(Object value) {
    if (value != null) {
      if (value instanceof Number || value instanceof UUID || value instanceof URI) {
        return value.toString();
      } else if (value instanceof Date) {
        return toISO8601Date((Date) value);
      } else if (value instanceof String) {
        return cleanString((String) value);
      } else if (value instanceof Enum<?>) {
        return ((Enum<?>)value).name();
      }
    }
    return null;
  }

  /**
   * Returns the Event.VocabularyConcept.concept if it is not null, null otherwise.
   */
  private static String getConcept(Event.VocabularyConcept vocabularyConcept) {
    if (vocabularyConcept != null) {
      return vocabularyConcept.getConcept();
    }
    return null;
  }

  /**
   * Transform a local date data type into a String.
   */
  private static String getLocalDateValue(Date value) {
    if (value != null) {
      return toLocalISO8601Date(value);
    }
    return null;
  }

  /**
   * Validates if the occurrence record it's a repatriated record.
   */
  private static Optional<String> getRepatriated(Occurrence occurrence) {
    return getRepatriated(occurrence.getPublishingCountry(), occurrence.getCountry());
  }

  /**
   * Validates if the event record it's a repatriated record.
   */
  private static Optional<String> getRepatriated(Event event) {
    return getRepatriated(event.getPublishingCountry(), event.getCountry());
  }

  /**
   * Validates if the occurrence record it's a repatriated record.
   */
  private static Optional<String> getRepatriated(Country publishingCountry, Country country) {
    if (publishingCountry != null && country != null) {
      return Optional.of(Boolean.toString(country != publishingCountry));
    }
    return Optional.empty();
  }

  /**
   * If present, populates the GADM gid and name.
   */
  private static void putGadmFeature(Map<String,String> interpretedOccurrence, GadmTerm nameTerm, GadmTerm gidTerm, GadmFeature gadmFeature) {
    Optional.ofNullable(gadmFeature).ifPresent(gf -> {
      interpretedOccurrence.put(nameTerm.simpleName(), gf.getName());
      interpretedOccurrence.put(gidTerm.simpleName(), gf.getGid());
    });
  }

  /**
   * Extracts the agentIdentifier types from the record.
   */
  private static Optional<String> extractAgentIds(List<AgentIdentifier> agents) {
    return Optional.ofNullable(agents)
      .map(a -> a.stream().map(AgentIdentifier::getValue)
        .collect(Collectors.joining(";")));
  }

  /**
   * Extracts the media types from the record.
   */
  private static Optional<String> extractMediaTypes(List<MediaObject> mediaObjects) {
    return  Optional.ofNullable(mediaObjects)
              .map(media -> media.stream().filter(mediaObject -> Objects.nonNull(mediaObject.getType()))
                              .map(mediaObject -> mediaObject.getType().name())
                              .distinct()
                              .collect(Collectors.joining(";")));
  }

  private static String getSimpleValueAndNormalizeDelimiters(String value) {
    String simpleValue = getSimpleValue(value);

    if (simpleValue == null || simpleValue.isEmpty()) {
      return simpleValue;
    }

    return simpleValue.replace("|", ";");
  }

  /**
   * Extracts the spatial issues from the record.
   */
  private static Optional<String> extractOccurrenceIssues(Set<OccurrenceIssue> issues) {
    return  Optional.ofNullable(issues)
                .map(issuesSet -> issuesSet.stream().map(OccurrenceIssue::name)
                                 .collect(Collectors.joining(";")));
  }

  /**
   * Extract all the verbatim data into a Map.
   */
  public static Map<String, String> buildVerbatimOccurrenceMap(VerbatimOccurrence verbatimOccurrence) {
    HashMap<String, String> verbatimMap = new HashMap<>();
    TermUtils.verbatimTerms().forEach( term -> verbatimMap.put(term.simpleName(), cleanString(verbatimOccurrence.getVerbatimField(term))));
    return verbatimMap;
  }

  /**
   * Removes all delimiters in a string.
   */
  private static String cleanString(String value) {
    return Optional.ofNullable(value).map(v -> DELIMETERS_MATCH_PATTERN.matcher(v).replaceAll(" ")).orElse(value);
  }

  /**
   * Converts a date object into a String in IS0 8601 format.
   */
  protected static String toISO8601Date(Date date) {
    return date != null ? DownloadUtils.ISO_8601_ZONED.format(date.toInstant().atZone(ZoneOffset.UTC)) : null;
  }

  /**
   * Converts a date object into a String in IS0 8601 format, without timezone.
   */
  protected static String toLocalISO8601Date(Date date) {
    return date != null ? DownloadUtils.ISO_8601_LOCAL.format(date.toInstant().atZone(ZoneOffset.UTC)) : null;
  }
}
