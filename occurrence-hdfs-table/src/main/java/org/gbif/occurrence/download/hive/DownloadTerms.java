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
package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Definitions of terms used in downloading, and in create tables used during the download process.
 */
public class DownloadTerms {

  /**
   * Whether the term is from the verbatim fields, or the interpreted fields.
   */
  public enum Group {
    VERBATIM,
    INTERPRETED
  }

  /**
   * Terms not used in HDFS or downloads.
   */
  public static final Set<Term> EXCLUSIONS_INTERPRETED = ImmutableSet.of(
    GbifTerm.gbifID, // returned multiple times, so excluded and treated by adding once at the beginning
    GbifInternalTerm.fragmentHash, // omitted entirely
    GbifInternalTerm.fragment, // omitted entirely
    GbifTerm.numberOfOccurrences
  );

  /**
   * This set is used for the HDFS table definition
   */
  public static final Set<Term> EXCLUSIONS_HDFS = new ImmutableSet.Builder<Term>()
    .addAll(EXCLUSIONS_INTERPRETED)
    .add(GbifTerm.verbatimScientificName).build();

  /**
   * Terms present in HDFS but not included in a DWCA download
   */
  public static final Set<Term> EXCLUSIONS_DWCA_DOWNLOAD =
      new ImmutableSet.Builder<Term>()
          .add(GbifTerm.geologicalTime)
          .add(GbifTerm.lithostratigraphy)
          .add(GbifTerm.biostratigraphy)
          .add(GbifTerm.dnaSequenceID)
          .add(GbifTerm.checklistKey)
          .build();

  /**
   * Terms not used in DWCA downloads.
   */
  public static final Set<Term> EXCLUSIONS_DOWNLOAD =
      new ImmutableSet.Builder<Term>()
          .addAll(EXCLUSIONS_INTERPRETED)
          .addAll(EXCLUSIONS_DWCA_DOWNLOAD)
          .build();

  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS_HDFS =
    Sets.difference(ImmutableSet.copyOf(TermUtils.interpretedTerms()), EXCLUSIONS_HDFS).immutableCopy();

  /**
   * The interpreted terms included in a DWCA download.
   */
  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS =
    Sets.difference(ImmutableSet.copyOf(TermUtils.interpretedTerms()), EXCLUSIONS_DOWNLOAD).immutableCopy();

  /**
   * The interpreted terms included in a DWCA download, with GBIFID first.
   */
  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID =
    Sets.difference(ImmutableSet.copyOf(TermUtils.interpretedTerms()), EXCLUSIONS_DWCA_DOWNLOAD)
      .immutableCopy();

  /*
   * The verbatim terms included in a DWCA download.
   */
  public static final Set<Term> DOWNLOAD_VERBATIM_TERMS =
    Sets.difference(ImmutableSet.copyOf(TermUtils.verbatimTerms()), EXCLUSIONS_HDFS).immutableCopy();

  /*
   * The multimedia terms included in a DWCA download.
   */
  public static final Set<Term> DOWNLOAD_MULTIMEDIA_TERMS =
    Sets.difference(ImmutableSet.copyOf(TermUtils.multimediaTerms()), EXCLUSIONS_HDFS).immutableCopy();

  /**
   * The terms that will be included in the interpreted table if also present in ${@link
   * org.gbif.occurrence.common.TermUtils#interpretedTerms()}
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
   * The additional terms that will be included in the SIMPLE_WITH_VERBATIM_AVRO download
   */
  private static final Set<Pair<Group, Term>> ADDITIONAL_SIMPLE_WITH_VERBATIM_DOWNLOAD_TERMS = ImmutableSet.of(
    Pair.of(Group.INTERPRETED, GbifTerm.kingdomKey),
    Pair.of(Group.INTERPRETED, GbifTerm.phylumKey),
    Pair.of(Group.INTERPRETED, GbifTerm.classKey),
    Pair.of(Group.INTERPRETED, GbifTerm.orderKey),
    Pair.of(Group.INTERPRETED, GbifTerm.familyKey),
    Pair.of(Group.INTERPRETED, GbifTerm.genusKey),
    Pair.of(Group.INTERPRETED, GbifTerm.publishingCountry));

  /**
   * The terms that will be included in the SIMPLE_WITH_VERBATIM_AVRO download
   */
  public static final Set<Pair<Group, Term>> SIMPLE_WITH_VERBATIM_DOWNLOAD_TERMS = Sets.union(
    SIMPLE_DOWNLOAD_TERMS, ADDITIONAL_SIMPLE_WITH_VERBATIM_DOWNLOAD_TERMS).immutableCopy();

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
    Pair.of(Group.INTERPRETED, IucnTerm.iucnRedListCategory),
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
    Pair.of(Group.INTERPRETED, GbifTerm.speciesKey),
    Pair.of(Group.INTERPRETED, IucnTerm.iucnRedListCategory)
  );

  /**
   * GBIF-Internal terms included in downloads (so why are they internal?)
   */
  public static final Set<Term> INTERNAL_DOWNLOAD_TERMS = ImmutableSet.of(
    GbifInternalTerm.publishingOrgKey,
    IucnTerm.iucnRedListCategory
  );

  /**
   * GBIF-Internal terms available for public searches (SQL downloads)
   */
  public static final Set<Term> INTERNAL_SEARCH_TERMS = ImmutableSet.of(
    GbifInternalTerm.publishingOrgKey,
    GbifInternalTerm.installationKey,
    GbifInternalTerm.institutionKey,
    GbifInternalTerm.collectionKey,
    GbifInternalTerm.programmeAcronym,
    GbifInternalTerm.hostingOrganizationKey,
    GbifInternalTerm.isInCluster,
    GbifInternalTerm.dwcaExtension,
    GbifInternalTerm.eventDateGte,
    GbifInternalTerm.eventDateLte
  );

  /*
   * The terms available for searching/selecting for occurrence SQL downloads.
   */
  public static final Set<Term> DOWNLOAD_SQL_VERBATIM_TERMS = DOWNLOAD_VERBATIM_TERMS;

  /*
   * The terms available for searching/selecting for occurrence SQL downloads.
   */
  public static final Set<Term> DOWNLOAD_SQL_TERMS = new ImmutableSet.Builder<Term>()
    .addAll(DOWNLOAD_INTERPRETED_TERMS)
    .addAll(DOWNLOAD_MULTIMEDIA_TERMS)
    .addAll(INTERNAL_DOWNLOAD_TERMS)
    .addAll(INTERNAL_SEARCH_TERMS)
    .add(GbifTerm.verbatimScientificName).build();

  public static String simpleName(Pair<Group, Term> termPair) {
    Term term = termPair.getRight();
    if (termPair.getLeft().equals(Group.VERBATIM)) {
      return "verbatim" + Character.toUpperCase(term.simpleName().charAt(0)) + term.simpleName().substring(1);
    } else {
      return term.simpleName();
    }
  }
}
