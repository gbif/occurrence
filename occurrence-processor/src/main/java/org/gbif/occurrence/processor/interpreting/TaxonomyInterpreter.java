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
package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.exception.UnparsableException;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.checklistbank.NameParser;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchKVStoreFactory;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.nameparser.NameParserGbifV1;
import org.gbif.occurrence.processor.conf.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.clients.SpeciesWsClient;
import org.gbif.rest.client.configuration.ChecklistbankClientsConfiguration;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.inject.Inject;

/**
 * Takes a VerbatimOccurrence and does nub lookup on its provided taxonomy, then writes the result to the passed in
 * Occurrence.
 */
public class TaxonomyInterpreter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);
  private static final NameParser PARSER = new NameParserGbifV1();
  private static final RankParser RANK_PARSER = RankParser.getInstance();


  private final KeyValueStore<SpeciesMatchRequest, org.gbif.rest.client.species.NameUsageMatch> matchingWs;
  private final KeyValueStore<String,NameUsage> speciesWs;

  @Inject
  public TaxonomyInterpreter(String apiUrl) {
    ClientConfiguration clbClientConfiguration = ClientConfiguration.builder().withBaseApiUrl(apiUrl).build();
    matchingWs = NameUsageMatchKVStoreFactory.nameUsageMatchKVStore(ChecklistbankClientsConfiguration.builder()
                                                                      .checklistbankClientConfiguration(clbClientConfiguration)
                                                                      .nameUSageClientConfiguration(clbClientConfiguration)
                                                                      .build());

    speciesWs =
        new KeyValueStore<String, NameUsage>() {
          private SpeciesWsClient speciesWsClient =
              new ClientBuilder()
                  .withUrl(apiUrl)
                  .withObjectMapper(
                      JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
                  .withFormEncoder()
                  .build(SpeciesWsClient.class);

          @Override
          public NameUsage get(String nubKey) {
            return speciesWsClient.get(nubKey);
          }

          @Override
          public void close() throws IOException {
            // do nothing
          }
        };
  }

  public TaxonomyInterpreter(ApiClientConfiguration cfg) {
    this(cfg.url);
  }

  /**
   * Assembles the most complete scientific name based on full and individual name parts.
   * @param scientificName the full scientific name
   * @param genericName see GbifTerm.genericName
   * @param genus see DwcTerm.genus
   * @param specificEpithet see DwcTerm.specificEpithet
   * @param infraspecificEpithet see DwcTerm.infraspecificEpithet
   */
  public static String buildScientificName(String scientificName, String authorship, String genericName, String genus,
                                           String specificEpithet, String infraspecificEpithet) {
    String sciname = ClassificationUtils.clean(scientificName);
    if (sciname == null) {
      // handle case when the scientific name is null and only given as atomized fields: genus & speciesEpitheton
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

    } else if (!Strings.isNullOrEmpty(authorship) && !sciname.toLowerCase().contains(authorship.toLowerCase())) {
      sciname = sciname + " " + authorship;
    }

    return sciname;
  }

  private OccurrenceParseResult<NameUsageMatch> match(Map<Term, String> terms) {
    Rank rank = interpretRank(terms);
    return match(
        value(terms, DwcTerm.kingdom),
        value(terms, DwcTerm.phylum),
        value(terms, DwcTerm.class_),
        value(terms, DwcTerm.order),
        value(terms, DwcTerm.family),
        value(terms, DwcTerm.genus),
        value(terms, DwcTerm.scientificName),
        value(terms, DwcTerm.scientificNameAuthorship),
        value(terms, DwcTerm.genericName),
        value(terms, DwcTerm.specificEpithet),
        value(terms, DwcTerm.infraspecificEpithet),
        rank);
  }

  public OccurrenceParseResult<NameUsageMatch> match(String kingdom, String phylum, String clazz, String order,
                                                     String family, String genus, String scientificName,
                                                     String authorship, String genericName, String specificEpithet,
                                                     String infraspecificEpithet, Rank rank) {

    String cleanGenus = ClassificationUtils.clean(genus);
    String cleanGenericName = ClassificationUtils.clean(genericName);
    String cleanSpecificEpithet = ClassificationUtils.cleanAuthor(specificEpithet);
    String cleanInfraspecificEpithet = ClassificationUtils.cleanAuthor(infraspecificEpithet);
    String cleanAuthorship = ClassificationUtils.cleanAuthor(authorship);

    String sciname = buildScientificName(scientificName, cleanAuthorship, cleanGenericName, cleanGenus,
                                               cleanSpecificEpithet, cleanInfraspecificEpithet);
    OccurrenceParseResult<NameUsageMatch> result;

    SpeciesMatchRequest.Builder speciesRequestBuilder =

      SpeciesMatchRequest.builder()
      .withKingdom(ClassificationUtils.clean(kingdom))
      .withPhylum(ClassificationUtils.clean(phylum))
      .withClazz(ClassificationUtils.clean(clazz))
      .withOrder(ClassificationUtils.clean(order))
      .withFamily(ClassificationUtils.clean(family))
      .withGenus(cleanGenus)
      .withScientificName(sciname);

    if (rank != null) {
      speciesRequestBuilder.withRank(rank.name());
    }

    LOG.debug("Attempt to match name [{}]", sciname);


    try {
      NameUsageMatch lookup = toNameUsageMatch(matchingWs.get(speciesRequestBuilder.build()));


      result = OccurrenceParseResult.success(ParseResult.CONFIDENCE.DEFINITE, lookup);
      switch (lookup.getMatchType()) {
        case NONE:
          result = OccurrenceParseResult.fail(lookup, OccurrenceIssue.TAXON_MATCH_NONE);
          LOG.info("match for [{}] returned no match. Lookup note: [{}]", scientificName, lookup.getNote());
          break;
        case FUZZY:
          result.addIssue(OccurrenceIssue.TAXON_MATCH_FUZZY);
          LOG.debug("match for [{}] was fuzzy. Match note: [{}]", scientificName, lookup.getNote());
          break;
        case HIGHERRANK:
          result.addIssue(OccurrenceIssue.TAXON_MATCH_HIGHERRANK);
          LOG.debug("match for [{}] was to higher rank only. Match note: [{}]", scientificName, lookup.getNote());
          break;
      }
    } catch (Exception e) {
      // Log the error
      LOG.error("Failed WS call with {}", speciesRequestBuilder, e);
      result = OccurrenceParseResult.error(e);
    }

    return result;
  }

  private NameUsageMatch toNameUsageMatch(org.gbif.rest.client.species.NameUsageMatch match) {

    NameUsageMatch nameUsageMatch = new NameUsageMatch();

    if (match.getAcceptedUsage() != null) {
      nameUsageMatch.setScientificName(match.getAcceptedUsage().getName());
      nameUsageMatch.setAcceptedUsageKey(match.getAcceptedUsage().getKey());
      nameUsageMatch.setRank(match.getAcceptedUsage().getRank());
    } else {
      nameUsageMatch.setScientificName(match.getUsage().getName());
      nameUsageMatch.setRank(match.getUsage().getRank());
    }

    nameUsageMatch.setCanonicalName(ClassificationUtils.canonicalName(nameUsageMatch.getScientificName(), nameUsageMatch.getRank()));

    if (match.getDiagnostics().getAlternatives() != null) {
      nameUsageMatch.setAlternatives(match.getDiagnostics().getAlternatives().stream().map(this::toNameUsageMatch).collect(Collectors.toList()));
    }

    Optional.ofNullable(match.getAcceptedUsage()).map(RankedName::getKey).ifPresent(nameUsageMatch::setAcceptedUsageKey);


    nameUsageMatch.setUsageKey(match.getUsage().getKey());

    nameUsageMatch.setMatchType(match.getDiagnostics().getMatchType());
    nameUsageMatch.setStatus(match.getDiagnostics().getStatus());
    nameUsageMatch.setConfidence(match.getDiagnostics().getConfidence());


    match.getClassification().forEach( rankedName -> {
      if (Rank.KINGDOM == rankedName.getRank()) {
        nameUsageMatch.setKingdom(rankedName.getName());
        nameUsageMatch.setKingdomKey(rankedName.getKey());
      } else if (Rank.PHYLUM == rankedName.getRank()) {
        nameUsageMatch.setPhylum(rankedName.getName());
        nameUsageMatch.setPhylumKey(rankedName.getKey());
      } else if (Rank.CLASS == rankedName.getRank()) {
        nameUsageMatch.setClazz(rankedName.getName());
        nameUsageMatch.setClassKey(rankedName.getKey());
      } else if (Rank.ORDER == rankedName.getRank()) {
        nameUsageMatch.setOrder(rankedName.getName());
        nameUsageMatch.setOrderKey(rankedName.getKey());
      } else if (Rank.FAMILY == rankedName.getRank()) {
        nameUsageMatch.setFamily(rankedName.getName());
        nameUsageMatch.setFamilyKey(rankedName.getKey());
      } else if (Rank.GENUS == rankedName.getRank()) {
        nameUsageMatch.setGenus(rankedName.getName());
        nameUsageMatch.setGenusKey(rankedName.getKey());
      } else if (Rank.SUBGENUS == rankedName.getRank()) {
        nameUsageMatch.setSubgenus(rankedName.getName());
        nameUsageMatch.setSubgenusKey(rankedName.getKey());
      } else if (Rank.SPECIES == rankedName.getRank()) {
        nameUsageMatch.setSpecies(rankedName.getName());
        nameUsageMatch.setSpeciesKey(rankedName.getKey());
      }

    });

    return nameUsageMatch;
  }

  private void applyMatch(Occurrence occ, NameUsageMatch match, Collection<OccurrenceIssue> issues) {
    occ.setTaxonKey(match.getUsageKey());
    occ.setScientificName(match.getScientificName());
    occ.setTaxonRank(match.getRank());
    occ.setTaxonomicStatus(match.getStatus());

    // copy issues
    occ.getIssues().addAll(issues);

    //has an AcceptedUsageKey?
    if(Objects.nonNull(match.getAcceptedUsageKey())) {
       getNameUsage(match.getAcceptedUsageKey()).ifPresent(acceptedUsage -> {
         occ.setAcceptedTaxonKey(acceptedUsage.getKey());
         occ.setAcceptedScientificName(acceptedUsage.getScientificName());
       });
    } else {
      //By default use taxonKey and scientificName as the accepted values
      occ.setAcceptedTaxonKey(match.getUsageKey());
      occ.setAcceptedScientificName(match.getScientificName());
    }

    // parse name into pieces - we dont get them from the nub lookup
    try {
      ParsedName pn = PARSER.parse(match.getScientificName(), match.getRank());
      occ.setGenericName(pn.getGenusOrAbove());
      occ.setSpecificEpithet(pn.getSpecificEpithet());
      occ.setInfraspecificEpithet(pn.getInfraSpecificEpithet());
    } catch (UnparsableException e) {
      if (e.type.isParsable()) {
        LOG.warn("Fail to parse backbone {} name for occurrence {}: {}", e.type, occ.getKey(), e.name);
      }
    }

    for (Rank r : Rank.DWC_RANKS) {
      org.gbif.api.util.ClassificationUtils.setHigherRank(occ, r, match.getHigherRank(r));
      org.gbif.api.util.ClassificationUtils.setHigherRankKey(occ, r, match.getHigherRankKey(r));
    }
    LOG.debug("Occurrence {} matched to nub {} [{}]", occ.getKey(), occ.getScientificName(), occ.getTaxonKey());
  }

  /**
   * Gets a name usage by its key.
   */
  private Optional<NameUsage> getNameUsage(Integer nubKey) {
    try {
      return  Optional.ofNullable(speciesWs.get(nubKey.toString()));
    } catch (Exception ex) {
      // Log the error
      LOG.error("Error getting accepted name usage: {}", nubKey);
    }
    return Optional.empty();
  }

  private static String value(Map<Term, String> terms, Term term) {
    return terms.get(term);
  }
  private static boolean hasTerm(Map<Term, String> terms, Term term) {
    return !Strings.isNullOrEmpty(value(terms, term));
  }

  public void interpretTaxonomy(VerbatimOccurrence verbatim, Occurrence occ) {

    // try core taxon fields first
    OccurrenceParseResult<NameUsageMatch> matchPR = match(verbatim.getVerbatimFields());

    // apply taxonomy if we got a match
    if (matchPR.isSuccessful()) {
      applyMatch(occ, matchPR.getPayload(), matchPR.getIssues());
    } else {
      LOG.debug("No backbone match for occurrence {}", occ.getKey());
      occ.addIssue(OccurrenceIssue.TAXON_MATCH_NONE);
      // assign unknown kingdom
      applyKingdom(occ, Kingdom.INCERTAE_SEDIS);
    }
  }

  private static void applyKingdom(Occurrence occ, Kingdom k){
    occ.setTaxonKey(k.nubUsageKey());
    occ.setScientificName(k.scientificName());
    occ.setTaxonRank(Rank.KINGDOM);
  }

  private static Rank interpretRank(Map<Term, String> terms){
    Rank rank = null;
    if (hasTerm(terms, DwcTerm.taxonRank)) {
      rank = RANK_PARSER.parse(value(terms, DwcTerm.taxonRank)).getPayload();
    }
    // try again with verbatim if it exists
    if (rank == null && hasTerm(terms, DwcTerm.verbatimTaxonRank)) {
      rank = RANK_PARSER.parse(value(terms, DwcTerm.verbatimTaxonRank)).getPayload();
    }
    // derive from atomized fields
    if (rank == null && hasTerm(terms, DwcTerm.genus)) {
      if (hasTerm(terms, DwcTerm.specificEpithet)) {
        if (hasTerm(terms, DwcTerm.infraspecificEpithet)) {
          rank = Rank.INFRASPECIFIC_NAME;
        } else {
          rank = Rank.SPECIES;
        }
      } else {
        rank = Rank.GENUS;
      }
    }
    return rank;
  }
}
