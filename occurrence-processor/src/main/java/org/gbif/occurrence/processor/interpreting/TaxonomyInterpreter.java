package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.exception.UnparsableException;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.checklistbank.NameParser;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.nameparser.NameParserGbifV1;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.util.RetryingWebserviceClient;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a VerbatimOccurrence and does nub lookup on its provided taxonomy, then writes the result to the passed in
 * Occurrence.
 */
public class TaxonomyInterpreter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);
  private static final NameParser PARSER = new NameParserGbifV1();
  private static final RankParser RANK_PARSER = RankParser.getInstance();
  private static final String SPECIES_PATH = "species";
  private static final String MATCH_PATH = "species/match";

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private static final LoadingCache<WebResource, NameUsageMatch> MATCH_CACHE =
    CacheBuilder.newBuilder()
      .maximumSize(10_000)
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(NameUsageMatch.class, 5, 2000));

  private static final LoadingCache<WebResource, NameUsage> SPECIES_CACHE =
    CacheBuilder.newBuilder()
      .maximumSize(10_000)
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(NameUsage.class, 5, 1000));


  private final WebResource matchingWs;
  private final WebResource speciesWs;

  @Inject
  public TaxonomyInterpreter(WebResource apiBaseWs) {
    matchingWs = apiBaseWs.path(MATCH_PATH);
    speciesWs = apiBaseWs.path(SPECIES_PATH);
  }

  public TaxonomyInterpreter(ApiClientConfiguration cfg) {
    this(cfg.newApiClient());
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
        value(terms, GbifTerm.genericName),
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
    MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
    queryParams.add("kingdom", ClassificationUtils.clean(kingdom));
    queryParams.add("phylum",  ClassificationUtils.clean(phylum));
    queryParams.add("class",   ClassificationUtils.clean(clazz));
    queryParams.add("order",   ClassificationUtils.clean(order));
    queryParams.add("family",  ClassificationUtils.clean(family));
    queryParams.add("genus",  cleanGenus);

    queryParams.add("name",   sciname);
    if (rank != null) {
      queryParams.add("rank", rank.name());
    }

    LOG.debug("Attempt to match name [{}]", sciname);
    WebResource res = matchingWs.queryParams(queryParams);
    LOG.debug("WS call with: {}", res.getURI());
    try {
      NameUsageMatch lookup = MATCH_CACHE.get(res);
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
      LOG.error("Failed WS call with: {}", res.getURI());
      result = OccurrenceParseResult.error(e);
    }

    return result;
  }

  private void applyMatch(Occurrence occ, NameUsageMatch match, Collection<OccurrenceIssue> issues) {
    occ.setTaxonKey(match.getUsageKey());
    occ.setScientificName(match.getScientificName());
    occ.setTaxonRank(match.getRank());
    occ.setTaxonomicStatus(match.getStatus());

    // copy issues
    occ.getIssues().addAll(issues);
    Optional.ofNullable(match.getAcceptedUsageKey())
      .flatMap(this::getNameUsage)
      .ifPresent(acceptedUsage -> {
        occ.setAcceptedTaxonKey(acceptedUsage.getKey());
        occ.setAcceptedScientificName(acceptedUsage.getScientificName());
      });


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
    WebResource resource = speciesWs.path(nubKey.toString());
    try {
      return  Optional.ofNullable(SPECIES_CACHE.get(resource));
    } catch (Exception ex) {
      // Log the error
      LOG.error("Error getting accepted name usage: {}", resource.getURI());
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
