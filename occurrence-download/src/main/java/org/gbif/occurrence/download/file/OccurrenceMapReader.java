package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.net.URI;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;

import static org.gbif.occurrence.common.download.DownloadUtils.DELIMETERS_MATCH_PATTERN;

/**
 * Reads a occurrence record from HBase and return it in a Map<String,Object>.
 */
public class OccurrenceMapReader {

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

    //Base terms
    interpretedOccurrence.put(GbifTerm.gbifID.simpleName(), getSimpleValue(occurrence.getKey()));
    interpretedOccurrence.put(GbifTerm.datasetKey.simpleName(), getSimpleValue(occurrence.getDatasetKey()));
    interpretedOccurrence.put(GbifInternalTerm.publishingOrgKey.simpleName(), getSimpleValue(occurrence.getPublishingOrgKey()));
    interpretedOccurrence.put(GbifTerm.publishingCountry.simpleName(), getCountryCode(occurrence.getPublishingCountry()));
    interpretedOccurrence.put(GbifInternalTerm.installationKey.simpleName(), getSimpleValue(occurrence.getInstallationKey()));
    interpretedOccurrence.put(GbifInternalTerm.networkKey.simpleName(), joinUUIDs(occurrence.getNetworkKeys()));
    interpretedOccurrence.put(GbifTerm.lastCrawled.simpleName(), getSimpleValue(occurrence.getLastCrawled()));
    interpretedOccurrence.put(GbifTerm.lastParsed.simpleName(), getSimpleValue(occurrence.getLastParsed()));
    interpretedOccurrence.put(GbifTerm.lastInterpreted.simpleName(), getSimpleValue(occurrence.getLastInterpreted()));
    interpretedOccurrence.put(GbifTerm.protocol.simpleName(), getSimpleValue(occurrence.getProtocol()));
    interpretedOccurrence.put(GbifInternalTerm.crawlId.simpleName(), getSimpleValue(occurrence.getCrawlId()));
    Optional.ofNullable(occurrence.getVerbatimField(DcTerm.identifier))
      .ifPresent(x -> interpretedOccurrence.put(DcTerm.identifier.simpleName(), x));

    // taxonomy terms
    interpretedOccurrence.put(GbifTerm.taxonKey.simpleName(), getSimpleValue(occurrence.getTaxonKey()));
    interpretedOccurrence.put(GbifTerm.acceptedTaxonKey.simpleName(), getSimpleValue(occurrence.getAcceptedTaxonKey()));
    interpretedOccurrence.put(DwcTerm.scientificName.simpleName(), occurrence.getScientificName());
    interpretedOccurrence.put(GbifTerm.acceptedScientificName.simpleName(), occurrence.getAcceptedScientificName());
    interpretedOccurrence.put(GbifTerm.genericName.simpleName(), occurrence.getGenericName());
    interpretedOccurrence.put(DwcTerm.specificEpithet.simpleName(), occurrence.getSpecificEpithet());
    interpretedOccurrence.put(DwcTerm.infraspecificEpithet.simpleName(), occurrence.getInfraspecificEpithet());
    interpretedOccurrence.put(DwcTerm.taxonRank.simpleName(), getSimpleValue(occurrence.getTaxonRank()));
    interpretedOccurrence.put(DwcTerm.taxonomicStatus.simpleName(), getSimpleValue(occurrence.getTaxonomicStatus()));

    Rank.DWC_RANKS.forEach(rank -> {
                              Optional.ofNullable(ClassificationUtils.getHigherRankKey(occurrence, rank))
                                .ifPresent(rankKey -> interpretedOccurrence.put(rank2KeyTerm.get(rank).simpleName(), rankKey.toString()));
                              Optional.ofNullable(ClassificationUtils.getHigherRank(occurrence, rank))
                                .ifPresent(rankClassification -> interpretedOccurrence.put(rank2Term.get(rank).simpleName(), rankClassification));
                           });


    // other java properties
    interpretedOccurrence.put(DwcTerm.basisOfRecord.simpleName(), getSimpleValue(occurrence.getBasisOfRecord()));
    interpretedOccurrence.put(GbifTerm.elevation.simpleName(), getSimpleValue(occurrence.getElevation()));
    interpretedOccurrence.put(GbifTerm.elevationAccuracy.simpleName(), getSimpleValue(occurrence.getElevationAccuracy()));
    interpretedOccurrence.put(GbifTerm.depth.simpleName(), getSimpleValue(occurrence.getDepth()));
    interpretedOccurrence.put(GbifTerm.depthAccuracy.simpleName(), getSimpleValue(occurrence.getDepthAccuracy()));
    interpretedOccurrence.put(GbifTerm.depthAccuracy.simpleName(), getSimpleValue(occurrence.getDepthAccuracy()));
    interpretedOccurrence.put(GbifTerm.lastInterpreted.simpleName(), getSimpleValue(occurrence.getLastInterpreted()));
    interpretedOccurrence.put(DcTerm.modified.simpleName(),getSimpleValue( occurrence.getModified()));
    interpretedOccurrence.put(DwcTerm.dateIdentified.simpleName(), getSimpleValue(occurrence.getDateIdentified()));
    interpretedOccurrence.put(DwcTerm.decimalLatitude.simpleName(), getSimpleValue(occurrence.getDecimalLatitude()));
    interpretedOccurrence.put(DwcTerm.decimalLongitude.simpleName(), getSimpleValue(occurrence.getDecimalLongitude()));
    interpretedOccurrence.put(GbifTerm.coordinateAccuracy.simpleName(), getSimpleValue(occurrence.getCoordinateAccuracy()));
    interpretedOccurrence.put(DwcTerm.coordinatePrecision.simpleName(), getSimpleValue(occurrence.getCoordinatePrecision()));
    interpretedOccurrence.put(DwcTerm.coordinateUncertaintyInMeters.simpleName(), getSimpleValue(occurrence.getCoordinateUncertaintyInMeters()));
    interpretedOccurrence.put(DwcTerm.countryCode.simpleName(), getCountryCode(occurrence.getCountry()));
    interpretedOccurrence.put(DwcTerm.stateProvince.simpleName(), occurrence.getStateProvince());
    interpretedOccurrence.put(DwcTerm.continent.simpleName(), getSimpleValue(occurrence.getContinent()));
    interpretedOccurrence.put(DwcTerm.waterBody.simpleName(), occurrence.getWaterBody());
    interpretedOccurrence.put(DwcTerm.eventDate.simpleName(), getSimpleValue(occurrence.getEventDate()));
    interpretedOccurrence.put(DwcTerm.year.simpleName(), getSimpleValue(occurrence.getYear()));
    interpretedOccurrence.put(DwcTerm.month.simpleName(), getSimpleValue(occurrence.getMonth()));
    interpretedOccurrence.put(DwcTerm.day.simpleName(), getSimpleValue(occurrence.getDay()));
    interpretedOccurrence.put(DwcTerm.individualCount.simpleName(), getSimpleValue(occurrence.getIndividualCount()));
    interpretedOccurrence.put(DwcTerm.establishmentMeans.simpleName(), getSimpleValue(occurrence.getEstablishmentMeans()));
    interpretedOccurrence.put(DwcTerm.lifeStage.simpleName(), getSimpleValue(occurrence.getLifeStage()));
    interpretedOccurrence.put(DwcTerm.sex.simpleName(), getSimpleValue(occurrence.getSex()));
    interpretedOccurrence.put(DwcTerm.typeStatus.simpleName(), getSimpleValue(occurrence.getTypeStatus()));
    interpretedOccurrence.put(GbifTerm.typifiedName.simpleName(), occurrence.getTypifiedName());
    interpretedOccurrence.put(DcTerm.references.simpleName(), getSimpleValue(occurrence.getReferences()));
    interpretedOccurrence.put(DcTerm.license.simpleName(), getSimpleValue(occurrence.getLicense()));

    //Boolean flags
    interpretedOccurrence.put(GbifTerm.hasGeospatialIssues.simpleName(), Boolean.toString(occurrence.hasSpatialIssue()));
    interpretedOccurrence.put(GbifTerm.hasCoordinate.simpleName(), Boolean.toString(occurrence.getDecimalLatitude() != null && occurrence.getDecimalLongitude() != null));

    getRepatriated(occurrence).ifPresent(repatriated -> interpretedOccurrence.put(GbifTerm.repatriated.simpleName(), repatriated));
    extractOccurrenceIssues(occurrence).ifPresent(issues -> interpretedOccurrence.put(GbifTerm.issue.simpleName(), issues));
    extractMediaTypes(occurrence).ifPresent(mediaTypes -> interpretedOccurrence.put(GbifTerm.mediaType.simpleName(), mediaTypes));

    occurrence.getVerbatimFields().forEach( (term, value) -> {
      if (!INTERPRETED_SOURCE_TERMS.contains(term)) {
       interpretedOccurrence.put(term.simpleName(), value);
      }
    });

    return interpretedOccurrence;
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
    return  buildInterpretedOccurrenceMap(occurrence).entrySet().stream()
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
   * Validates if the occurrence record it's a repatriated record.
   */
  private static Optional<String> getRepatriated(Occurrence occurrence) {
    Country publishingCountry = occurrence.getPublishingCountry();
    Country countryCode = occurrence.getCountry();

    if (publishingCountry != null && countryCode != null) {
      return Optional.of(Boolean.toString(countryCode != publishingCountry));
    }
    return Optional.empty();
  }

  /**
   * Extracts the media types from the record.
   */
  private static Optional<String> extractMediaTypes(Occurrence occurrence) {
    return  Optional.ofNullable(occurrence.getMedia())
              .map(media -> media.stream().filter(mediaObject -> Objects.nonNull(mediaObject.getType()))
                              .map(mediaObject -> mediaObject.getType().name())
                              .distinct()
                              .collect(Collectors.joining(";")));
  }

  /**
   * Extracts the spatial issues from the record.
   */
  private static Optional<String> extractOccurrenceIssues(Occurrence occurrence) {
    return  Optional.ofNullable(occurrence.getIssues())
                .map(issues -> issues.stream().map(OccurrenceIssue::name)
                                 .collect(Collectors.joining(";")));
  }


  /**
   * Extract all the verbatim data into a Map.
   */
  public static Map<String, String> buildVerbatimOccurrenceMap(Occurrence occurrence) {
    HashMap<String, String> verbatimMap = new HashMap<>();
    TermUtils.verbatimTerms().forEach( term -> verbatimMap.put(term.simpleName(), cleanString(occurrence.getVerbatimField(term))));
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
    return date != null ? DownloadUtils.ISO_8601_FORMAT.format(date.toInstant().atZone(ZoneOffset.UTC)) : null;
  }

}
