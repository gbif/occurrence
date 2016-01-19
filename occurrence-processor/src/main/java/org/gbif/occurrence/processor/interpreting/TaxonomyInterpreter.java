package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.nameparser.NameParser;
import org.gbif.nameparser.UnparsableException;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.util.RetryingWebserviceClient;

import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MultivaluedMap;

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
public class TaxonomyInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);
  private static final NameParser parser = new NameParser();

  private static final String MATCH_PATH = "species/match";

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private static final LoadingCache<WebResource, NameUsageMatch> CACHE =
    CacheBuilder.newBuilder()
      .maximumSize(10000)
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(NameUsageMatch.class, 5, 2000));


  private final WebResource MATCHING_WS;

  @Inject
  public TaxonomyInterpreter(WebResource apiBaseWs) {
    MATCHING_WS = apiBaseWs.path(MATCH_PATH);
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
  public String buildScientificName(String scientificName, String genericName, String genus, String specificEpithet, String infraspecificEpithet) {
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
      sciname = pn.canonicalName();
    }
    return sciname;
  }

  public OccurrenceParseResult<NameUsageMatch> match(String kingdom, String phylum, String clazz, String order,
    String family, String genus, String scientificName, String specificEpithet, String infraspecificEpithet) {

    final String sciname = buildScientificName(scientificName, null, genus, specificEpithet, infraspecificEpithet);

    OccurrenceParseResult<NameUsageMatch> result;
    MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
    queryParams.add("kingdom", kingdom);
    queryParams.add("phylum", phylum);
    queryParams.add("class", clazz);
    queryParams.add("order", order);
    queryParams.add("family", family);
    queryParams.add("genus", genus);
    queryParams.add("name", sciname);

    LOG.debug("Attempt to match name [{}]", sciname);
    WebResource res = MATCHING_WS.queryParams(queryParams);
    LOG.info("WS call with: {}", res.getURI());
    try {
      NameUsageMatch lookup = CACHE.get(res);
      result = OccurrenceParseResult.success(ParseResult.CONFIDENCE.DEFINITE, lookup);
      switch (lookup.getMatchType()) {
        case NONE:
          result.addIssue(OccurrenceIssue.TAXON_MATCH_NONE);
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

  public void interpretTaxonomy(VerbatimOccurrence verbatim, Occurrence occ) {
    final String sciname = buildScientificName(verbatim.getVerbatimField(DwcTerm.scientificName),
      verbatim.getVerbatimField(GbifTerm.genericName), verbatim.getVerbatimField(DwcTerm.genus),
      verbatim.getVerbatimField(DwcTerm.specificEpithet), verbatim.getVerbatimField(DwcTerm.infraspecificEpithet));
    OccurrenceParseResult<NameUsageMatch> matchPR = match(
        ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.kingdom)),
        ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.phylum)),
        ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.class_)),
        ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.order)),
        ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.family)),
        ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.genus)), sciname,
        ClassificationUtils.cleanAuthor(verbatim.getVerbatimField(DwcTerm.specificEpithet)),
        ClassificationUtils.cleanAuthor(verbatim.getVerbatimField(DwcTerm.infraspecificEpithet)));

    if (!matchPR.isSuccessful()) {
      LOG.debug("Unsuccessful backbone match for occurrence {} with name {}", occ.getKey(), sciname);
      occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
      occ.addIssue(OccurrenceIssue.TAXON_MATCH_NONE);

    } else {
      NameUsageMatch match = matchPR.getPayload();
      occ.setTaxonKey(match.getUsageKey());
      occ.setScientificName(match.getScientificName());
      occ.setTaxonRank(match.getRank());
      // copy issues
      occ.getIssues().addAll(matchPR.getIssues());

      // parse name into pieces - we dont get them from the nub lookup
      try {
        ParsedName pn = parser.parse(match.getScientificName(), null);
        occ.setGenericName(pn.getGenusOrAbove());
        occ.setSpecificEpithet(pn.getSpecificEpithet());
        occ.setInfraspecificEpithet(pn.getInfraSpecificEpithet());
      } catch (UnparsableException e) {
        LOG.warn("Fail to parse backbone name {} for occurrence {}: {}", e.name, occ.getKey(), e.type);
      }

      for (Rank r : Rank.DWC_RANKS) {
        org.gbif.api.util.ClassificationUtils.setHigherRank(occ, r, match.getHigherRank(r));
        org.gbif.api.util.ClassificationUtils.setHigherRankKey(occ, r, match.getHigherRankKey(r));
      }
      LOG.debug("Occurrence {} matched to nub {}", occ.getKey(), occ.getScientificName());
    }
  }
}
