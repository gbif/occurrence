package org.gbif.occurrence.common;

import static org.gbif.occurrence.common.TermUtils.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.*;

/** This class customizes some of the methods and variables of TermUtils to apply them to events. */
public class EventTermUtils {

  public static final Set<Term> TERMS_POPULATED_BY_INTERPRETATION =
      ImmutableSet.of(
          DwcTerm.decimalLatitude,
          DwcTerm.decimalLongitude,
          DwcTerm.continent,
          DwcTerm.waterBody,
          DwcTerm.stateProvince,
          DwcTerm.countryCode,
          DwcTerm.dateIdentified,
          DwcTerm.eventDate,
          DwcTerm.year,
          DwcTerm.month,
          DwcTerm.day,
          DwcTerm.startDayOfYear,
          DwcTerm.endDayOfYear,
          GbifTerm.datasetKey,
          GbifTerm.publishingCountry,
          GbifTerm.lastInterpreted,
          DcTerm.modified,
          DwcTerm.coordinateUncertaintyInMeters,
          DwcTerm.coordinatePrecision,
          GbifTerm.elevation,
          GbifTerm.elevationAccuracy,
          DwcTerm.minimumDepthInMeters,
          DwcTerm.maximumDepthInMeters,
          DwcTerm.minimumElevationInMeters,
          DwcTerm.maximumElevationInMeters,
          GbifTerm.distanceFromCentroidInMeters,
          GbifTerm.depth,
          GbifTerm.depthAccuracy,
          GadmTerm.level0Gid,
          GadmTerm.level0Name,
          GadmTerm.level1Gid,
          GadmTerm.level1Name,
          GadmTerm.level2Gid,
          GadmTerm.level2Name,
          GadmTerm.level3Gid,
          GadmTerm.level3Name,
          DwcTerm.sampleSizeUnit,
          DwcTerm.sampleSizeValue,
          DwcTerm.organismQuantityType,
          DwcTerm.organismQuantity,
          GbifInternalTerm.unitQualifier,
          GbifTerm.issue,
          DwcTerm.recordedByID,
          DwcTerm.identifiedByID,
          DcTerm.references,
          GbifTerm.protocol,
          GbifTerm.lastCrawled,
          GbifTerm.lastParsed,
          GbifInternalTerm.installationKey,
          GbifInternalTerm.publishingOrgKey,
          GbifInternalTerm.networkKey,
          GbifTerm.mediaType,
          DcTerm.license,
          DwcTerm.datasetID,
          DwcTerm.datasetName,
          DwcTerm.otherCatalogNumbers,
          DwcTerm.recordedBy,
          DwcTerm.identifiedBy,
          DwcTerm.preparations,
          DwcTerm.samplingProtocol,
          GbifTerm.gbifRegion,
          GbifTerm.publishedByGbifRegion,
          DwcTerm.georeferencedBy,
          DwcTerm.higherGeography,
          GbifTerm.projectId,
          DwcTerm.eventType,
          DwcTerm.eventID,
          DwcTerm.parentEventID,
          DwcTerm.locality,
          DwcTerm.locationID);

  private static final Set<Term> TERMS_SUBJECT_TO_INTERPRETATION =
      ImmutableSet.<Term>builder()
          .addAll(TERMS_POPULATED_BY_INTERPRETATION)
          .add(
              DwcTerm.verbatimLatitude,
              DwcTerm.verbatimLongitude,
              DwcTerm.verbatimCoordinates,
              DwcTerm.geodeticDatum,
              DwcTerm.country,
              DcTerm.modified,
              DwcTerm.associatedMedia)
          .build();

  private static final Set<Term> TERMS_REMOVED_DURING_INTERPRETATION =
      Sets.difference(TERMS_SUBJECT_TO_INTERPRETATION, TERMS_POPULATED_BY_INTERPRETATION);

  public static List<Term> interpretedTerms() {
    return ImmutableList.<Term>builder()
        .add(GbifTerm.gbifID)
        .addAll(
            // add all Dublin Core terms that are not stripped during interpretation
            DwC_DC_PROPERTIES.stream()
                .filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t))
                .collect(Collectors.toList()))
        .addAll(TERMS_POPULATED_BY_INTERPRETATION)
        .addAll(
            // add all GADM terms (none are stripped during interpretation, but filter anyway).
            GADM_PROPERTIES.stream()
                .filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t))
                .collect(Collectors.toList()))
        .build();
  }

  public static List<Term> verbatimTerms() {
    return ImmutableList.<Term>builder()
        .add(GbifTerm.gbifID)
        .addAll(DwC_DC_PROPERTIES)
        .addAll(DwC_PROPERTIES)
        .build();
  }

  public static List<Term> internalTerms() {
    return ImmutableList.of(
        GbifInternalTerm.identifierCount,
        GbifInternalTerm.crawlId,
        GbifInternalTerm.fragmentCreated,
        GbifInternalTerm.xmlSchema,
        GbifInternalTerm.publishingOrgKey,
        GbifInternalTerm.unitQualifier,
        GbifInternalTerm.networkKey,
        GbifInternalTerm.installationKey,
        GbifInternalTerm.programmeAcronym,
        GbifInternalTerm.hostingOrganizationKey,
        GbifInternalTerm.dwcaExtension,
        GbifInternalTerm.datasetTitle,
        GbifInternalTerm.eventDateGte,
        GbifInternalTerm.eventDateLte,
        GbifInternalTerm.parentEventGbifId,
        GbifInternalTerm.humboldtEventDurationValueInMinutes);
  }

  public static boolean isInterpretedSourceTerm(Term term) {
    return TERMS_SUBJECT_TO_INTERPRETATION.contains(term);
  }
}
