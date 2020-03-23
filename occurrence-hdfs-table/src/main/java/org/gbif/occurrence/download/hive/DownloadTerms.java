package org.gbif.occurrence.download.hive;

import org.apache.commons.lang3.tuple.Pair;
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

  /**
   * Whether the term is from the verbatim fields, or the intepreted fields.
   */
  public enum Group {
    VERBATIM,
    INTERPRETED
  }

  //This list of exclusion is used for the download query only
  public static final Set<Term> EXCLUSIONS_INTERPRETED = ImmutableSet.of(GbifTerm.gbifID,
                                                                   // returned multiple times, so excluded and treated by adding once at the beginning
                                                                   GbifInternalTerm.fragmentHash,
                                                                   // omitted entirely
                                                                   GbifInternalTerm.fragment,
                                                                   // omitted entirely
                                                                   GbifTerm.numberOfOccurrences,
                                                                   //this is for aggregation only
                                                                   GbifTerm.verbatimScientificName
                                                                   //Does not need to be included since it exists as verbatim
                                                                   );

  //This set is used fot the HDFS table definition
  public static final Set<Term> EXCLUSIONS = new ImmutableSet.Builder<Term>().addAll(EXCLUSIONS_INTERPRETED).build();

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
  public static final Set<Pair<Group, Term>> SIMPLE_DOWNLOAD_TERMS = ImmutableSet.of(
    Pair.of(Group.INTERPRETED, GbifTerm.gbifID),
    Pair.of(Group.INTERPRETED, GbifTerm.datasetKey),
    Pair.of(Group.INTERPRETED, DwcTerm.occurrenceID),
    Pair.of(Group.INTERPRETED, DwcTerm.kingdom),
    Pair.of(Group.INTERPRETED, DwcTerm.phylum),
    Pair.of(Group.INTERPRETED, DwcTerm.class_),
    Pair.of(Group.INTERPRETED, DwcTerm.order),
    Pair.of(Group.INTERPRETED, DwcTerm.family),
    Pair.of(Group.INTERPRETED, DwcTerm.genus),
    Pair.of(Group.INTERPRETED, GbifTerm.species),
    Pair.of(Group.INTERPRETED, DwcTerm.infraspecificEpithet),
    Pair.of(Group.INTERPRETED, DwcTerm.taxonRank),
    Pair.of(Group.INTERPRETED, DwcTerm.scientificName),
    Pair.of(Group.VERBATIM, DwcTerm.scientificName),
    Pair.of(Group.VERBATIM, DwcTerm.scientificNameAuthorship),
    Pair.of(Group.INTERPRETED, DwcTerm.countryCode),
    Pair.of(Group.INTERPRETED, DwcTerm.locality),
    Pair.of(Group.INTERPRETED, DwcTerm.stateProvince),
    Pair.of(Group.INTERPRETED, DwcTerm.occurrenceStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.individualCount),
    Pair.of(Group.INTERPRETED, GbifInternalTerm.publishingOrgKey),
    Pair.of(Group.INTERPRETED, DwcTerm.decimalLatitude),
    Pair.of(Group.INTERPRETED, DwcTerm.decimalLongitude),
    Pair.of(Group.INTERPRETED, DwcTerm.coordinateUncertaintyInMeters),
    Pair.of(Group.INTERPRETED, DwcTerm.coordinatePrecision),
    Pair.of(Group.INTERPRETED, GbifTerm.elevation),
    Pair.of(Group.INTERPRETED, GbifTerm.elevationAccuracy),
    Pair.of(Group.INTERPRETED, GbifTerm.depth),
    Pair.of(Group.INTERPRETED, GbifTerm.depthAccuracy),
    Pair.of(Group.INTERPRETED, DwcTerm.eventDate),
    Pair.of(Group.INTERPRETED, DwcTerm.day),
    Pair.of(Group.INTERPRETED, DwcTerm.month),
    Pair.of(Group.INTERPRETED, DwcTerm.year),
    Pair.of(Group.INTERPRETED, GbifTerm.taxonKey),
    Pair.of(Group.INTERPRETED, GbifTerm.speciesKey),
    Pair.of(Group.INTERPRETED, DwcTerm.basisOfRecord),
    Pair.of(Group.INTERPRETED, DwcTerm.institutionCode),
    Pair.of(Group.INTERPRETED, DwcTerm.collectionCode),
    Pair.of(Group.INTERPRETED, DwcTerm.catalogNumber),
    Pair.of(Group.INTERPRETED, DwcTerm.recordNumber),
    Pair.of(Group.INTERPRETED, DwcTerm.identifiedBy),
    Pair.of(Group.INTERPRETED, DwcTerm.dateIdentified),
    Pair.of(Group.INTERPRETED, DcTerm.license),
    Pair.of(Group.INTERPRETED, DcTerm.rightsHolder),
    Pair.of(Group.INTERPRETED, DwcTerm.recordedBy),
    Pair.of(Group.INTERPRETED, DwcTerm.typeStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.establishmentMeans),
    Pair.of(Group.INTERPRETED, GbifTerm.lastInterpreted),
    Pair.of(Group.INTERPRETED, GbifTerm.mediaType),
    Pair.of(Group.INTERPRETED, GbifTerm.issue)
  );

  /**
   * The terms that will be included in the species list
   */
  public static final Set<Pair<Group, Term>> SPECIES_LIST_TERMS = ImmutableSet.of(
    Pair.of(Group.INTERPRETED, GbifTerm.gbifID),
    Pair.of(Group.INTERPRETED, GbifTerm.datasetKey),
    Pair.of(Group.INTERPRETED, DwcTerm.occurrenceID),
    Pair.of(Group.INTERPRETED, GbifTerm.taxonKey),
    Pair.of(Group.INTERPRETED, GbifTerm.acceptedTaxonKey),
    Pair.of(Group.INTERPRETED, GbifTerm.acceptedScientificName),
    Pair.of(Group.INTERPRETED, DwcTerm.scientificName),
    Pair.of(Group.INTERPRETED, DwcTerm.taxonRank),
    Pair.of(Group.INTERPRETED, DwcTerm.taxonomicStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.kingdom),
    Pair.of(Group.INTERPRETED, GbifTerm.kingdomKey),
    Pair.of(Group.INTERPRETED, DwcTerm.phylum),
    Pair.of(Group.INTERPRETED, GbifTerm.phylumKey),
    Pair.of(Group.INTERPRETED, DwcTerm.class_),
    Pair.of(Group.INTERPRETED, GbifTerm.classKey),
    Pair.of(Group.INTERPRETED, DwcTerm.order),
    Pair.of(Group.INTERPRETED, GbifTerm.orderKey),
    Pair.of(Group.INTERPRETED, DwcTerm.family),
    Pair.of(Group.INTERPRETED, GbifTerm.familyKey),
    Pair.of(Group.INTERPRETED, DwcTerm.genus),
    Pair.of(Group.INTERPRETED, GbifTerm.genusKey),
    Pair.of(Group.INTERPRETED, GbifTerm.species),
    Pair.of(Group.INTERPRETED, GbifTerm.speciesKey),
    Pair.of(Group.INTERPRETED, DcTerm.license)
  );

  /**
   * The terms that will be included in the species list for downloads
   */
  public static final Set<Pair<Group, Term>> SPECIES_LIST_DOWNLOAD_TERMS = ImmutableSet.of(
    Pair.of(Group.INTERPRETED, GbifTerm.taxonKey),
    Pair.of(Group.INTERPRETED, DwcTerm.scientificName),
    Pair.of(Group.INTERPRETED, GbifTerm.acceptedTaxonKey),
    Pair.of(Group.INTERPRETED, GbifTerm.acceptedScientificName),
    Pair.of(Group.INTERPRETED, GbifTerm.numberOfOccurrences),
    Pair.of(Group.INTERPRETED, DwcTerm.taxonRank),
    Pair.of(Group.INTERPRETED, DwcTerm.taxonomicStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.kingdom),
    Pair.of(Group.INTERPRETED, GbifTerm.kingdomKey),
    Pair.of(Group.INTERPRETED, DwcTerm.phylum),
    Pair.of(Group.INTERPRETED, GbifTerm.phylumKey),
    Pair.of(Group.INTERPRETED, DwcTerm.class_),
    Pair.of(Group.INTERPRETED, GbifTerm.classKey),
    Pair.of(Group.INTERPRETED, DwcTerm.order),
    Pair.of(Group.INTERPRETED, GbifTerm.orderKey),
    Pair.of(Group.INTERPRETED, DwcTerm.family),
    Pair.of(Group.INTERPRETED, GbifTerm.familyKey),
    Pair.of(Group.INTERPRETED, DwcTerm.genus),
    Pair.of(Group.INTERPRETED, GbifTerm.genusKey),
    Pair.of(Group.INTERPRETED, GbifTerm.species),
    Pair.of(Group.INTERPRETED, GbifTerm.speciesKey)
  );

  public static String simpleName(Pair<Group, Term> termPair) {
    Term term = termPair.getRight();
    if (termPair.getLeft().equals(Group.VERBATIM)) {
      return "verbatim" + Character.toUpperCase(term.simpleName().charAt(0)) + term.simpleName().substring(1);
    } else {
      return term.simpleName();
    }
  }
}
