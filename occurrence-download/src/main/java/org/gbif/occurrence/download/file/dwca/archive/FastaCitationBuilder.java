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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.CompoundPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.query.TitleLookupService;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds the download-level citation string for FASTA Archive downloads.
 *
 * <p>Format: FASTA Archive Download of {target_gene} sequences from {taxon_scope} in
 * {geographic_scope}. GBIF.org ({date}). {doi}
 *
 * <p>The "of {target_gene} sequences", "from {taxon_scope}", and "in {geographic_scope}" segments
 * are each omitted when no corresponding filter was applied to the download request.
 *
 * <p>Examples:
 * <ul>
 *   <li>FASTA Archive Download of COI sequences. GBIF.org (16 March 2026). https://doi.org/10.15468/dl.xxxxx
 *   <li>FASTA Archive Download of COI sequences from Insecta in Norway. GBIF.org (16 March 2026). https://doi.org/10.15468/dl.xxxxx
 *   <li>FASTA Archive Download of rbcL sequences from Tracheophyta. GBIF.org (16 March 2026). https://doi.org/10.15468/dl.xxxxx
 * </ul>
 */
@Slf4j
@UtilityClass
public class FastaCitationBuilder {

  private static final String DATE_FORMAT = "d MMMM yyyy";

  private static final Set<OccurrenceSearchParameter> TAXON_KEY_PARAMS = Set.of(
    OccurrenceSearchParameter.TAXON_KEY,
    OccurrenceSearchParameter.ACCEPTED_TAXON_KEY,
    OccurrenceSearchParameter.KINGDOM_KEY,
    OccurrenceSearchParameter.PHYLUM_KEY,
    OccurrenceSearchParameter.CLASS_KEY,
    OccurrenceSearchParameter.ORDER_KEY,
    OccurrenceSearchParameter.FAMILY_KEY,
    OccurrenceSearchParameter.GENUS_KEY,
    OccurrenceSearchParameter.SUBGENUS_KEY,
    OccurrenceSearchParameter.SPECIES_KEY
  );

  private static final Set<OccurrenceSearchParameter> GADM_PARAMS = Set.of(
    OccurrenceSearchParameter.GADM_GID,
    OccurrenceSearchParameter.GADM_LEVEL_0_GID,
    OccurrenceSearchParameter.GADM_LEVEL_1_GID,
    OccurrenceSearchParameter.GADM_LEVEL_2_GID,
    OccurrenceSearchParameter.GADM_LEVEL_3_GID
  );

  /**
   * Builds the citation string for the given FASTA Archive download.
   *
   * @return the formatted citation, or null if the download request is not a PredicateDownloadRequest
   */
  public static String buildCitation(Download download, TitleLookupService titleLookupService) {
    if (!(download.getRequest() instanceof PredicateDownloadRequest)) {
      return null;
    }
    Predicate predicate = ((PredicateDownloadRequest) download.getRequest()).getPredicate();

    List<String> targetGenes = new ArrayList<>();
    List<String> taxonScopes = new ArrayList<>();
    List<String> geoScopes = new ArrayList<>();

    collectValues(predicate, targetGenes, taxonScopes, geoScopes, titleLookupService);

    StringBuilder citation = new StringBuilder("FASTA Archive Download");

    if (!targetGenes.isEmpty()) {
      citation.append(" of ").append(String.join(", ", targetGenes)).append(" sequences");
    } else if (!taxonScopes.isEmpty() || !geoScopes.isEmpty()) {
      citation.append(" of sequences");
    }
    if (!taxonScopes.isEmpty()) {
      citation.append(" from ").append(String.join(", ", taxonScopes));
    }
    if (!geoScopes.isEmpty()) {
      citation.append(" in ").append(String.join(", ", geoScopes));
    }

    String date = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH).format(download.getCreated());
    String doi = download.getDoi() != null
      ? download.getDoi().getUrl().toString()
      : download.getKey();

    citation.append(". GBIF.org (").append(date).append("). ").append(doi);

    return citation.toString();
  }

  private static void collectValues(
    Predicate predicate,
    List<String> targetGenes,
    List<String> taxonScopes,
    List<String> geoScopes,
    TitleLookupService titleLookupService
  ) {
    if (predicate == null) {
      return;
    }
    if (predicate instanceof EqualsPredicate) {
      EqualsPredicate<?> eq = (EqualsPredicate<?>) predicate;
      if (eq.getKey() instanceof OccurrenceSearchParameter) {
        addValue(
          (OccurrenceSearchParameter) eq.getKey(),
          Collections.singletonList(eq.getValue()),
          targetGenes, taxonScopes, geoScopes, titleLookupService);
      }
    } else if (predicate instanceof InPredicate) {
      InPredicate<?> in = (InPredicate<?>) predicate;
      if (in.getKey() instanceof OccurrenceSearchParameter) {
        addValue(
          (OccurrenceSearchParameter) in.getKey(),
          new ArrayList<>(in.getValues()),
          targetGenes, taxonScopes, geoScopes, titleLookupService);
      }
    } else if (predicate instanceof WithinPredicate) {
      geoScopes.add("a geographic area");
    } else if (predicate instanceof CompoundPredicate) {
      ((CompoundPredicate) predicate).getPredicates()
        .forEach(p -> collectValues(p, targetGenes, taxonScopes, geoScopes, titleLookupService));
    } else if (predicate instanceof NotPredicate) {
      // negated predicates are not meaningful for a citation scope summary
    }
  }

  private static void addValue(
    OccurrenceSearchParameter param,
    List<String> values,
    List<String> targetGenes,
    List<String> taxonScopes,
    List<String> geoScopes,
    TitleLookupService titleLookupService
  ) {
    if (param == OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_TARGET_GENE) {
      targetGenes.addAll(values);

    } else if (TAXON_KEY_PARAMS.contains(param)) {
      values.forEach(v -> {
        try {
          taxonScopes.add(titleLookupService.getSpeciesName(v));
        } catch (Exception e) {
          log.warn("Could not look up species name for key {}", v);
          taxonScopes.add(v);
        }
      });

    } else if (param == OccurrenceSearchParameter.SCIENTIFIC_NAME) {
      taxonScopes.addAll(values);

    } else if (param == OccurrenceSearchParameter.COUNTRY) {
      values.forEach(v -> {
        Country c = Country.fromIsoCode(v);
        geoScopes.add(c != null ? c.getTitle() : v);
      });

    } else if (param == OccurrenceSearchParameter.CONTINENT) {
      values.forEach(v -> {
        try {
          geoScopes.add(Continent.valueOf(v).getTitle());
        } catch (IllegalArgumentException e) {
          geoScopes.add(v);
        }
      });

    } else if (GADM_PARAMS.contains(param)) {
      geoScopes.addAll(values);
    }
  }
}
