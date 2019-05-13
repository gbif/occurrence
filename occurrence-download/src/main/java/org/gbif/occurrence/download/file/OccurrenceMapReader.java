package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.download.DownloadUtils;

import java.net.URI;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import static org.gbif.occurrence.common.download.DownloadUtils.DELIMETERS_MATCH_PATTERN;

/**
 * Reads a occurrence record from HBase and return it in a Map<String,Object>.
 */
public class OccurrenceMapReader {


  public static Map<String, String> buildOccurrenceMap(Occurrence occurrence) {

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
    interpretedOccurrence.put(GbifTerm.protocol.simpleName(), getSimpleValue(occurrence.getProtocol()));
    interpretedOccurrence.put(GbifInternalTerm.crawlId.simpleName(), getSimpleValue(occurrence.getCrawlId()));



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

    Rank.DWC_RANKS.forEach(rank ->
      Optional.ofNullable(ClassificationUtils.getHigherRankKey(occurrence, rank))
        .ifPresent( rankKey -> interpretedOccurrence.put(OccurrenceBuilder.rank2KeyTerm.get(rank).simpleName(), rankKey.toString()))
    );


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


    interpretedOccurrence.put(GbifTerm.hasGeospatialIssues.simpleName(), Boolean.toString(occurrence.hasSpatialIssue()));
    interpretedOccurrence.put(GbifTerm.hasCoordinate.simpleName(), Boolean.toString(occurrence.getDecimalLatitude() != null && occurrence.getDecimalLongitude() != null));

    getRepatriated(occurrence).ifPresent(repatriated -> interpretedOccurrence.put(GbifTerm.repatriated.simpleName(), repatriated));
    extractOccurrenceIssues(occurrence).ifPresent(issues -> interpretedOccurrence.put(GbifTerm.issue.simpleName(), issues));
    extractMediaTypes(occurrence).ifPresent(mediaTypes -> interpretedOccurrence.put(GbifTerm.mediaType.simpleName(), mediaTypes));



    return interpretedOccurrence;
  }


  public static Map<String, String> buildOccurrenceMap(Occurrence occurrence, Collection<Term> terms) {
    return  buildOccurrenceMap(occurrence).entrySet().stream()
              .filter(entry -> terms.stream().anyMatch(term -> term.simpleName().equals(entry.getKey())))
              .collect(HashMap::new, (m,v)->m.put(v.getKey(), v.getValue()), HashMap::putAll);
  }


  private static String joinUUIDs(Collection<UUID> uuids) {
    if (uuids != null ) {
     return uuids.stream().map(UUID::toString).collect(Collectors.joining(";"));
    }
    return null;
  }

  private static String getCountryCode(Country country) {
    if (country != null) {
      return country.getIso2LetterCode();
    }
    return null;
  }


  private static String getSimpleValue(Object value) {
    if (value != null) {
      if (value instanceof Date) {
        return toISO8601Date((Date) value);
      } else if (value instanceof Number) {
        return value.toString();
      } else if (value instanceof String) {
        return cleanString((String) value);
      } else if (value instanceof Enum<?>) {
        return ((Enum<?>)value).name();
      } else if (value instanceof UUID) {
        return ((UUID)value).toString();
      } else if (value instanceof URI) {
        return ((URI)value).toString();
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
   * Extracts the media types from the hbase result.
   */
  private static Optional<String> extractMediaTypes(Occurrence occurrence) {
    return  Optional.ofNullable(occurrence.getMedia())
              .map(media -> media.stream().map(mediaObject -> mediaObject.getType().name())
                              .collect(Collectors.joining(";")));
  }

  /**
   * Extracts the spatial issues from the hbase result.
   */
  private static Optional<String> extractOccurrenceIssues(Occurrence occurrence) {
    return  Optional.ofNullable(occurrence.getIssues())
                .map(issues -> issues.stream().map(OccurrenceIssue::name)
                                 .collect(Collectors.joining(";")));
  }


  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildVerbatimOccurrenceMap(@Nullable Occurrence occurrence) {
    return occurrence.getVerbatimFields().entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().simpleName(), Map.Entry::getValue));
  }


  private static String cleanString(String value) {
    return Optional.ofNullable(value).map(v -> DELIMETERS_MATCH_PATTERN.matcher(v).replaceAll(" ")).orElse(value);
  }

  /**
   * Converts a date object into a String in IS0 8601 format.
   */
  public static String toISO8601Date(Date date) {
    return date != null ? DownloadUtils.ISO_8601_FORMAT.format(date.toInstant().atZone(ZoneOffset.UTC)) : null;
  }

}
