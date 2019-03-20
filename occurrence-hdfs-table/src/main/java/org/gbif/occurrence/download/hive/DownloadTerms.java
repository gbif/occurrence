package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Definitions of terms used in downloading, and in create tables used during the download process.
 */
public class DownloadTerms {

  //This list of exclusion is used for the download query only
  public static final Set<Term> EXCLUSIONS_INTERPRETED = ImmutableSet.of(GbifTerm.gbifID,
                                                                   // returned multiple times, so excluded and treated by adding once at the beginning
                                                                   GbifInternalTerm.fragmentHash,
                                                                   // omitted entirely
                                                                   GbifInternalTerm.fragment,
                                                                   // omitted entirely
                                                                   GbifTerm.numberOfOccurrences
                                                                   //this is for aggregation only
  );

  //This set is used fot the HDFS table definition
  //GbifTerm.mediaType handled as extension
  public static final Set<Term> EXCLUSIONS = new ImmutableSet.Builder<Term>().addAll(EXCLUSIONS_INTERPRETED)
                                                  .add(GbifTerm.mediaType).build();

  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS_HDFS =
    Sets.difference(ImmutableSet.copyOf(Terms.interpretedTerms()), EXCLUSIONS).immutableCopy();

  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS =
    Sets.difference(ImmutableSet.copyOf(Terms.interpretedTerms()), EXCLUSIONS_INTERPRETED).immutableCopy();

  public static final Set<Term> DOWNLOAD_VERBATIM_TERMS =
    Sets.difference(ImmutableSet.copyOf(Terms.verbatimTerms()), EXCLUSIONS).immutableCopy();
  /**
   * The terms that will be included in the interpreted table if also present in ${@link
   * org.gbif.occurrence.download.hive.Terms#interpretedTerms()}
   */
  public static final Set<Term> SIMPLE_DOWNLOAD_TERMS = ImmutableSet.of(GbifTerm.gbifID,
                                                                        GbifTerm.datasetKey,
                                                                        DwcTerm.occurrenceID,
                                                                        DwcTerm.kingdom,
                                                                        DwcTerm.phylum,
                                                                        DwcTerm.class_,
                                                                        DwcTerm.order,
                                                                        DwcTerm.family,
                                                                        DwcTerm.genus,
                                                                        GbifTerm.species,
                                                                        DwcTerm.infraspecificEpithet,
                                                                        DwcTerm.taxonRank,
                                                                        DwcTerm.scientificName,
                                                                        DwcTerm.countryCode,
                                                                        DwcTerm.locality,
                                                                        GbifInternalTerm.publishingOrgKey,
                                                                        DwcTerm.decimalLatitude,
                                                                        DwcTerm.decimalLongitude,
                                                                        DwcTerm.coordinateUncertaintyInMeters,
                                                                        DwcTerm.coordinatePrecision,
                                                                        GbifTerm.elevation,
                                                                        GbifTerm.elevationAccuracy,
                                                                        GbifTerm.depth,
                                                                        GbifTerm.depthAccuracy,
                                                                        DwcTerm.eventDate,
                                                                        DwcTerm.day,
                                                                        DwcTerm.month,
                                                                        DwcTerm.year,
                                                                        GbifTerm.taxonKey,
                                                                        GbifTerm.speciesKey,
                                                                        DwcTerm.basisOfRecord,
                                                                        DwcTerm.institutionCode,
                                                                        DwcTerm.collectionCode,
                                                                        DwcTerm.catalogNumber,
                                                                        DwcTerm.recordNumber,
                                                                        DwcTerm.identifiedBy,
                                                                        DwcTerm.dateIdentified,
                                                                        DcTerm.license,
                                                                        DcTerm.rightsHolder,
                                                                        DwcTerm.recordedBy,
                                                                        DwcTerm.typeStatus,
                                                                        DwcTerm.establishmentMeans,
                                                                        GbifTerm.lastInterpreted,
                                                                        GbifTerm.mediaType,
                                                                        GbifTerm.issue);
  
  /**
   * The terms that will be included in the species list
   */
  public static final Set<Term> SPECIES_LIST_TERMS = ImmutableSet.of(GbifTerm.gbifID,
                                                                    GbifTerm.datasetKey,
                                                                    DwcTerm.occurrenceID,
                                                                    GbifTerm.taxonKey,
                                                                    GbifTerm.acceptedTaxonKey,
                                                                    GbifTerm.acceptedScientificName,
                                                                    DwcTerm.scientificName,
                                                                    DwcTerm.taxonRank,
                                                                    DwcTerm.taxonomicStatus,
                                                                    DwcTerm.kingdom,
                                                                    GbifTerm.kingdomKey,
                                                                    DwcTerm.phylum,
                                                                    GbifTerm.phylumKey,
                                                                    DwcTerm.class_,
                                                                    GbifTerm.classKey,
                                                                    DwcTerm.order,
                                                                    GbifTerm.orderKey,
                                                                    DwcTerm.family,
                                                                    GbifTerm.familyKey,
                                                                    DwcTerm.genus,
                                                                    GbifTerm.genusKey,
                                                                    GbifTerm.species,
                                                                    GbifTerm.speciesKey,
                                                                    DcTerm.license);
  
  /**
   * The terms that will be included in the species list for downloads
   */
  public static final Set<Term> SPECIES_LIST_DOWNLOAD_TERMS = ImmutableSet.of(GbifTerm.taxonKey,
                                                                              DwcTerm.scientificName,
                                                                              GbifTerm.acceptedTaxonKey,
                                                                              GbifTerm.acceptedScientificName,
                                                                              GbifTerm.numberOfOccurrences,
                                                                              DwcTerm.taxonRank,
                                                                              DwcTerm.taxonomicStatus,
                                                                              DwcTerm.kingdom,
                                                                              GbifTerm.kingdomKey,
                                                                              DwcTerm.phylum,
                                                                              GbifTerm.phylumKey,
                                                                              DwcTerm.class_,
                                                                              GbifTerm.classKey,
                                                                              DwcTerm.order,
                                                                              GbifTerm.orderKey,
                                                                              DwcTerm.family,
                                                                              GbifTerm.familyKey,
                                                                              DwcTerm.genus,
                                                                              GbifTerm.genusKey,
                                                                              GbifTerm.species,
                                                                              GbifTerm.speciesKey);
}
