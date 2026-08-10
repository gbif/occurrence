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
package org.gbif.occurrence.trino.processor.interpreters;

import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.conf.CachedRestKVStoreConfiguration;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.occurrence.trino.processor.result.NameUsageMatchResult;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a VerbatimOccurrence and does nub lookup on its provided taxonomy, then writes the result
 * to the passed in Occurrence.
 */
public class TaxonomyInterpreter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);

  // we use COL as default
  private static final String DEFAULT_CHECKLIST_KEY = "7ddf754f-d193-4cc9-b351-99906754a03b";

  private final KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> matchingWs;

  public TaxonomyInterpreter(String apiMatchingServiceUrl) {
    ClientConfiguration matchingWsClientConfiguration =
        ClientConfiguration.builder().withBaseApiUrl(apiMatchingServiceUrl).build();

    matchingWs =
        NameUsageMatchKVStoreFactory.nameUsageMatchRestKVStoreCaffeine(
            CachedRestKVStoreConfiguration.builder().build(), matchingWsClientConfiguration);
  }

  public OccurrenceParseResult<NameUsageMatchResult> match(
      String checklistKey,
      String kingdom,
      String phylum,
      String clazz,
      String order,
      String family,
      String genus,
      String scientificName,
      String authorship,
      String genericName,
      String specificEpithet,
      String infraspecificEpithet,
      Rank rank) {

    String cleanGenus = ClassificationUtils.clean(genus);
    String cleanGenericName = ClassificationUtils.clean(genericName);
    String cleanSpecificEpithet = ClassificationUtils.cleanAuthor(specificEpithet);
    String cleanInfraspecificEpithet = ClassificationUtils.cleanAuthor(infraspecificEpithet);
    String cleanAuthorship = ClassificationUtils.cleanAuthor(authorship);

    String sciname =
        buildScientificName(
            scientificName,
            cleanAuthorship,
            cleanGenericName,
            cleanGenus,
            cleanSpecificEpithet,
            cleanInfraspecificEpithet);
    OccurrenceParseResult<NameUsageMatchResult> result;

    NameUsageMatchRequest.NameUsageMatchRequestBuilder nameUsageMatchRequestBuilder =
        NameUsageMatchRequest.builder()
            .withChecklistKey(
                checklistKey != null && !checklistKey.isEmpty()
                    ? checklistKey
                    : DEFAULT_CHECKLIST_KEY)
            .withKingdom(ClassificationUtils.clean(kingdom))
            .withPhylum(ClassificationUtils.clean(phylum))
            .withClazz(ClassificationUtils.clean(clazz))
            .withOrder(ClassificationUtils.clean(order))
            .withFamily(ClassificationUtils.clean(family))
            .withGenus(cleanGenus)
            .withScientificName(sciname);

    if (rank != null) {
      nameUsageMatchRequestBuilder.withTaxonRank(rank.name());
    }

    LOG.debug("Attempt to match name [{}]", sciname);

    try {
      NameUsageMatchResponse nameUsageMatchResponse =
          matchingWs.get(nameUsageMatchRequestBuilder.build());
      NameUsageMatchResult nameUsageMatchResult = new NameUsageMatchResult(nameUsageMatchResponse);

      result = OccurrenceParseResult.success(ParseResult.CONFIDENCE.DEFINITE, nameUsageMatchResult);
      if (nameUsageMatchResponse.getDiagnostics() != null) {
        if (nameUsageMatchResponse.getDiagnostics().getMatchType()
            == NameUsageMatchResponse.MatchType.NONE) {
          result = OccurrenceParseResult.fail(nameUsageMatchResult);
          LOG.info(
              "match for [{}] returned no match. Lookup note: [{}]",
              scientificName,
              nameUsageMatchResponse.getDiagnostics().getNote());
        } else {
          LOG.debug(
              "match for [{}] was {}. Match note: [{}]",
              scientificName,
              nameUsageMatchResponse.getDiagnostics().getMatchType(),
              nameUsageMatchResponse.getDiagnostics().getNote());
        }
      }
    } catch (Exception e) {
      // Log the error
      LOG.error("Failed WS call with {}", nameUsageMatchRequestBuilder, e);
      result = OccurrenceParseResult.error(e);
    }

    return result;
  }

  /**
   * Assembles the most complete scientific name based on full and individual name parts.
   *
   * @param scientificName the full scientific name
   * @param genericName see GbifTerm.genericName
   * @param genus see DwcTerm.genus
   * @param specificEpithet see DwcTerm.specificEpithet
   * @param infraspecificEpithet see DwcTerm.infraspecificEpithet
   */
  private static String buildScientificName(
      String scientificName,
      String authorship,
      String genericName,
      String genus,
      String specificEpithet,
      String infraspecificEpithet) {
    String sciname = ClassificationUtils.clean(scientificName);
    if (sciname == null) {
      // handle case when the scientific name is null and only given as atomized fields: genus &
      // speciesEpitheton
      ParsedName pn = new ParsedName();
      if (!StringUtils.isBlank(genericName)) {
        pn.setGenusOrAbove(genericName);
      } else {
        pn.setGenusOrAbove(genus);
      }
      pn.setSpecificEpithet(specificEpithet);
      pn.setInfraSpecificEpithet(infraspecificEpithet);
      pn.setAuthorship(authorship);
      sciname = pn.canonicalNameComplete();

    } else if (!Strings.isNullOrEmpty(authorship)
        && !sciname.toLowerCase().contains(authorship.toLowerCase())) {
      sciname = sciname + " " + authorship;
    }

    return sciname;
  }
}
