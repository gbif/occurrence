package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.common.search.solr.QueryUtils;
import org.gbif.common.search.solr.SolrConstants;
import org.gbif.occurrence.search.solr.FacetField;
import org.gbif.occurrence.search.solr.FacetFieldConfiguration;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;
import org.gbif.occurrence.search.solr.SolrQueryUtils;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.solr.client.solrj.SolrQuery;

import static org.gbif.common.search.solr.QueryUtils.PARAMS_JOINER;
import static org.gbif.common.search.solr.QueryUtils.PARAMS_OR_JOINER;
import static org.gbif.common.search.solr.QueryUtils.setQueryPaging;
import static org.gbif.common.search.solr.QueryUtils.setSortOrder;
import static org.gbif.common.search.solr.QueryUtils.taggedField;
import static org.gbif.common.search.solr.QueryUtils.toParenthesesQuery;
import static org.gbif.common.search.solr.SearchDateUtils.toDateQuery;
import static org.gbif.common.search.solr.SolrConstants.DEFAULT_QUERY;
import static org.gbif.common.search.solr.SolrConstants.PARAM_OR_OP;


/**
 * Utility class for building Solr queries from supported parameters for occurrences search.
 */
public class OccurrenceSearchRequestBuilder {

  private static final String SOLR_SPELLCHECK = "spellcheck";
  private static final String SOLR_SPELLCHECK_COUNT = "spellcheck.count";
  private static final String SOLR_SPELLCHECK_Q = "spellcheck.q";
  private static final String DEFAULT_SPELL_CHECK_COUNT = "4";
  private static final Pattern COMMON_REPLACER = Pattern.compile(",", Pattern.LITERAL);


  /**
   * Utility class to generates full text queries.
   */
  private static final class OccurrenceFullTextQueryBuilder {


    private String q;

    private static final Double FUZZY_DISTANCE = 0.8;

    private static final String TERM_PATTERN = "%1$s^%2$s %1$s~%3$s^%4$s";

    private static final String NON_TOKENIZED_QUERY_PATTERN = ":%1$s^1000";

    private static final Set<String> NON_TOKENIZABLE_FIELDS =
      ImmutableSet.<String>of(OccurrenceSolrField.CATALOG_NUMBER.getFieldName() + NON_TOKENIZED_QUERY_PATTERN,
                              OccurrenceSolrField.OCCURRENCE_ID.getFieldName() + NON_TOKENIZED_QUERY_PATTERN,
                              OccurrenceSolrField.SCIENTIFIC_NAME.getFieldName() + NON_TOKENIZED_QUERY_PATTERN);


    private static final String NON_TOKENIZED_QUERY = PARAMS_OR_JOINER.join(NON_TOKENIZABLE_FIELDS);

    private static final Integer MAX_SCORE = 100;

    private static final Integer SCORE_DECREMENT = 20;


    /**
     * Query parameter.
     */
    private OccurrenceFullTextQueryBuilder withQ(String q) {
      this.q = q;
      return this;
    }


    /**
     * Builds a Solr expression query with the form: "term1 ..termN" term1^100 term1~0.7^50 ... termN^20 termN~0.7^10.
     * Each boosting parameter is calculated using the formula:  MAX_SCORE - SCORE_DECREMENT * i. Where 'i' is the
     * position of the term in the query.
     */
    public String build() {
      String[] qs = q.split(" ");
      if(qs.length > 1) {
        StringBuilder ftQ = new StringBuilder(qs.length);
        String phraseQ = QueryUtils.toPhraseQuery(q);
        ftQ.append(phraseQ);
        ftQ.append(' ');
        for (int i = 0; i < qs.length; i++) {
          if (qs[i].length() > 1) { //ignore tokens of single letters
            int termScore = Math.max(MAX_SCORE - (SCORE_DECREMENT * i), SCORE_DECREMENT);
            ftQ.append(String.format(TERM_PATTERN, qs[i], termScore, FUZZY_DISTANCE, termScore / 2));
            if (i < (qs.length - 1)) {
              ftQ.append(' ');
            }
          }
        }
        return PARAMS_OR_JOINER.join(String.format(NON_TOKENIZED_QUERY, phraseQ), ftQ.toString());
      }
      return  PARAMS_OR_JOINER.join(String.format(NON_TOKENIZED_QUERY, q),
                                    String.format(TERM_PATTERN, q, MAX_SCORE, FUZZY_DISTANCE, MAX_SCORE / 2));
    }
  }

  // This is a placeholder to map from the JSON definition ID to the query field
  public static final ImmutableMap<OccurrenceSearchParameter, OccurrenceSolrField> QUERY_FIELD_MAPPING =
    ImmutableMap.<OccurrenceSearchParameter, OccurrenceSolrField>builder()
      .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, OccurrenceSolrField.LATITUDE)
      .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, OccurrenceSolrField.LONGITUDE)
      .put(OccurrenceSearchParameter.YEAR, OccurrenceSolrField.YEAR)
      .put(OccurrenceSearchParameter.MONTH, OccurrenceSolrField.MONTH)
      .put(OccurrenceSearchParameter.CATALOG_NUMBER, OccurrenceSolrField.CATALOG_NUMBER)
      .put(OccurrenceSearchParameter.RECORDED_BY, OccurrenceSolrField.RECORDED_BY)
      .put(OccurrenceSearchParameter.RECORD_NUMBER, OccurrenceSolrField.RECORD_NUMBER)
      .put(OccurrenceSearchParameter.COLLECTION_CODE, OccurrenceSolrField.COLLECTION_CODE)
      .put(OccurrenceSearchParameter.INSTITUTION_CODE, OccurrenceSolrField.INSTITUTION_CODE)
      .put(OccurrenceSearchParameter.DEPTH, OccurrenceSolrField.DEPTH)
      .put(OccurrenceSearchParameter.ELEVATION, OccurrenceSolrField.ELEVATION)
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD, OccurrenceSolrField.BASIS_OF_RECORD)
      .put(OccurrenceSearchParameter.DATASET_KEY, OccurrenceSolrField.DATASET_KEY)
      .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, OccurrenceSolrField.SPATIAL_ISSUES)
      .put(OccurrenceSearchParameter.HAS_COORDINATE, OccurrenceSolrField.HAS_COORDINATE)
      .put(OccurrenceSearchParameter.EVENT_DATE, OccurrenceSolrField.EVENT_DATE)
      .put(OccurrenceSearchParameter.LAST_INTERPRETED, OccurrenceSolrField.LAST_INTERPRETED)
      .put(OccurrenceSearchParameter.COUNTRY, OccurrenceSolrField.COUNTRY)
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, OccurrenceSolrField.PUBLISHING_COUNTRY)
      .put(OccurrenceSearchParameter.CONTINENT, OccurrenceSolrField.CONTINENT)
      .put(OccurrenceSearchParameter.TAXON_KEY, OccurrenceSolrField.TAXON_KEY)
      .put(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY, OccurrenceSolrField.ACCEPTED_TAXON_KEY)
      .put(OccurrenceSearchParameter.TAXONOMIC_STATUS, OccurrenceSolrField.TAXONOMIC_STATUS)
      .put(OccurrenceSearchParameter.KINGDOM_KEY, OccurrenceSolrField.KINGDOM_KEY)
      .put(OccurrenceSearchParameter.PHYLUM_KEY, OccurrenceSolrField.PHYLUM_KEY)
      .put(OccurrenceSearchParameter.CLASS_KEY, OccurrenceSolrField.CLASS_KEY)
      .put(OccurrenceSearchParameter.ORDER_KEY, OccurrenceSolrField.ORDER_KEY)
      .put(OccurrenceSearchParameter.FAMILY_KEY, OccurrenceSolrField.FAMILY_KEY)
      .put(OccurrenceSearchParameter.GENUS_KEY, OccurrenceSolrField.GENUS_KEY)
      .put(OccurrenceSearchParameter.SUBGENUS_KEY, OccurrenceSolrField.SUBGENUS_KEY)
      .put(OccurrenceSearchParameter.SPECIES_KEY, OccurrenceSolrField.SPECIES_KEY)
      .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, OccurrenceSolrField.SCIENTIFIC_NAME)
      .put(OccurrenceSearchParameter.TYPE_STATUS, OccurrenceSolrField.TYPE_STATUS)
      .put(OccurrenceSearchParameter.MEDIA_TYPE, OccurrenceSolrField.MEDIA_TYPE)
      .put(OccurrenceSearchParameter.ISSUE, OccurrenceSolrField.ISSUE)
      .put(OccurrenceSearchParameter.OCCURRENCE_ID, OccurrenceSolrField.OCCURRENCE_ID)
      .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, OccurrenceSolrField.ESTABLISHMENT_MEANS)
      .put(OccurrenceSearchParameter.REPATRIATED, OccurrenceSolrField.REPATRIATED)
      .put(OccurrenceSearchParameter.LOCALITY, OccurrenceSolrField.LOCALITY)
      .put(OccurrenceSearchParameter.STATE_PROVINCE, OccurrenceSolrField.STATE_PROVINCE)
      .put(OccurrenceSearchParameter.WATER_BODY, OccurrenceSolrField.WATER_BODY)
      .put(OccurrenceSearchParameter.LICENSE, OccurrenceSolrField.LICENSE)
      .put(OccurrenceSearchParameter.PROTOCOL, OccurrenceSolrField.PROTOCOL)
      .put(OccurrenceSearchParameter.ORGANISM_ID, OccurrenceSolrField.ORGANISM_ID)
      .put(OccurrenceSearchParameter.PUBLISHING_ORG, OccurrenceSolrField.PUBLISHING_ORGANIZATION_KEY)
      .put(OccurrenceSearchParameter.CRAWL_ID, OccurrenceSolrField.CRAWL_ID)
      .put(OccurrenceSearchParameter.INSTALLATION_KEY, OccurrenceSolrField.INSTALLATION_KEY)
      .put(OccurrenceSearchParameter.NETWORK_KEY, OccurrenceSolrField.NETWORK_KEY)
      .put(OccurrenceSearchParameter.EVENT_ID, OccurrenceSolrField.EVENT_ID)
      .put(OccurrenceSearchParameter.PARENT_EVENT_ID, OccurrenceSolrField.PARENT_EVENT_ID)
      .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, OccurrenceSolrField.SAMPLING_PROTOCOL)
      .build();

  public static final String GEO_INTERSECTS_QUERY_FMT = "\"Intersects(%s) distErrPct=0\"";

  public static final String BBOX_QUERY_FMT = "[%s TO %s]";

  // Solr full text search handle
  private static final String FULL_TEXT_HANDLER = "/search";

  // Holds the value used for an optional sort order applied to a search via param "sort"
  private final Map<String, SolrQuery.ORDER> sortOrder;

  private final int maxOffset;

  private final int maxLimit;

  //Flag to enable/disable faceted search
  private final boolean facetsEnable;

  public static final int MAX_OFFSET = 1000000;
  public static final int MAX_PAGE_SIZE = 300;

  private static final Map<OccurrenceSearchParameter,FacetFieldConfiguration> FACET_FIELD_CONFIGURATION_MAP =
    getFacetsConfiguration();

  /**
   * Default constructor.
   */
  public OccurrenceSearchRequestBuilder(Map<String, SolrQuery.ORDER> sortOrder, int maxOffset,
                                        int maxLimit, boolean facetsEnable) {
    Preconditions.checkArgument(maxOffset > 0, "Max offset must be greater than zero");
    Preconditions.checkArgument(maxLimit > 0, "Max limit must be greater than zero");
    this.sortOrder = sortOrder;
    this.maxOffset = Math.min(maxOffset, MAX_OFFSET);
    this.maxLimit = Math.min(maxLimit, MAX_PAGE_SIZE);
    this.facetsEnable = facetsEnable;
  }

  /**
   * Parses a geometry parameter in WKT format.
   * If the parsed geometry is a polygon the produced query will be in INTERSECTS(wkt parameter) format.
   * If the parsed geometry is a rectangle, the query is transformed into a range query using the southmost and
   * northmost points.
   */
  protected static String parseGeometryParam(String wkt) {
    try {
      Geometry geometry = new WKTReader().read(wkt);
      return String.format(GEO_INTERSECTS_QUERY_FMT, geometry.toText());
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public SolrQuery build(@Nullable OccurrenceSearchRequest request) {
    Preconditions.checkArgument(request.getOffset() <= maxOffset - request.getLimit(),
                                "maximum offset allowed is %s", this.maxOffset);

    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setParam(SOLR_SPELLCHECK, request.isSpellCheck());
    if (request.isSpellCheck()) {
      solrQuery.setParam(SOLR_SPELLCHECK_COUNT,
                         request.getSpellCheckCount() < 0
                           ? DEFAULT_SPELL_CHECK_COUNT
                           : Integer.toString(request.getSpellCheckCount()));
    }
    // q param
    if (Strings.isNullOrEmpty(request.getQ()) || SolrConstants.DEFAULT_FILTER_QUERY.equals(request.getQ())) {
      solrQuery.setQuery(DEFAULT_QUERY);
      solrQuery.setParam(SOLR_SPELLCHECK, Boolean.FALSE);
      // sorting is set only when the q parameter is empty, otherwise the score value es used
      setSortOrder(solrQuery, sortOrder);
    } else {
      OccurrenceFullTextQueryBuilder occurrenceFullTextQueryBuilder = new OccurrenceFullTextQueryBuilder();
      occurrenceFullTextQueryBuilder.withQ(request.getQ());
      solrQuery.setQuery(occurrenceFullTextQueryBuilder.build());
      solrQuery.setParam(SOLR_SPELLCHECK_Q, request.getQ());
      solrQuery.setRequestHandler(FULL_TEXT_HANDLER);
    }
    // paging
    setQueryPaging(request, solrQuery, maxLimit);
    // sets the filters
    setFilterParameters(request, solrQuery, facetsEnable);

    if (facetsEnable) {
      SolrQueryUtils.applyFacetSettings(request, solrQuery, FACET_FIELD_CONFIGURATION_MAP);
      solrQuery.setFacetMissing(false);
    }
    return solrQuery;
  }

  private static Map<OccurrenceSearchParameter, FacetFieldConfiguration> getFacetsConfiguration() {
    return ImmutableMap.<OccurrenceSearchParameter, FacetFieldConfiguration>builder()
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.BASIS_OF_RECORD).getFieldName(),
                                       OccurrenceSearchParameter.BASIS_OF_RECORD, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.TYPE_STATUS,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.TYPE_STATUS).getFieldName(),
                                       OccurrenceSearchParameter.TYPE_STATUS, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.DATASET_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.DATASET_KEY).getFieldName(),
                                       OccurrenceSearchParameter.DATASET_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.TAXON_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.TAXON_KEY).getFieldName(),
                                       OccurrenceSearchParameter.TAXON_KEY, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.COUNTRY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.COUNTRY).getFieldName(),
                                       OccurrenceSearchParameter.COUNTRY, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.MONTH,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.MONTH).getFieldName(),
                                       OccurrenceSearchParameter.MONTH, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.YEAR,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.YEAR).getFieldName(),
                                       OccurrenceSearchParameter.YEAR, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.KINGDOM_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.KINGDOM_KEY).getFieldName(),
                                       OccurrenceSearchParameter.KINGDOM_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.PHYLUM_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.PHYLUM_KEY).getFieldName(),
                                       OccurrenceSearchParameter.PHYLUM_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.CLASS_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.CLASS_KEY).getFieldName(),
                                       OccurrenceSearchParameter.CLASS_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.ORDER_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.ORDER_KEY).getFieldName(),
                                       OccurrenceSearchParameter.ORDER_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.FAMILY_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.FAMILY_KEY).getFieldName(),
                                       OccurrenceSearchParameter.FAMILY_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.GENUS_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.GENUS_KEY).getFieldName(),
                                       OccurrenceSearchParameter.GENUS_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.SUBGENUS_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.SUBGENUS_KEY).getFieldName(),
                                       OccurrenceSearchParameter.SUBGENUS_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.SPECIES_KEY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.SPECIES_KEY).getFieldName(),
                                       OccurrenceSearchParameter.SPECIES_KEY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.REPATRIATED,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.REPATRIATED).getFieldName(),
                                       OccurrenceSearchParameter.REPATRIATED, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.HAS_COORDINATE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.HAS_COORDINATE).getFieldName(),
                                       OccurrenceSearchParameter.HAS_COORDINATE, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE).getFieldName(),
                                       OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.CATALOG_NUMBER,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.CATALOG_NUMBER).getFieldName(),
                                       OccurrenceSearchParameter.CATALOG_NUMBER, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.COLLECTION_CODE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.COLLECTION_CODE).getFieldName(),
                                       OccurrenceSearchParameter.COLLECTION_CODE, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.CONTINENT,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.CONTINENT).getFieldName(),
                                       OccurrenceSearchParameter.CONTINENT, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.DECIMAL_LATITUDE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.DECIMAL_LATITUDE).getFieldName(),
                                       OccurrenceSearchParameter.DECIMAL_LATITUDE, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.DECIMAL_LONGITUDE).getFieldName(),
                                       OccurrenceSearchParameter.DECIMAL_LONGITUDE, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.RECORDED_BY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.RECORDED_BY).getFieldName(),
                                       OccurrenceSearchParameter.RECORDED_BY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.RECORD_NUMBER,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.RECORD_NUMBER).getFieldName(),
                                       OccurrenceSearchParameter.RECORD_NUMBER, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.PUBLISHING_COUNTRY).getFieldName(),
                                       OccurrenceSearchParameter.PUBLISHING_COUNTRY, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.DEPTH,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.DEPTH).getFieldName(),
                                       OccurrenceSearchParameter.DEPTH, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.ELEVATION,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.ELEVATION).getFieldName(),
                                       OccurrenceSearchParameter.ELEVATION, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.OCCURRENCE_ID,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.OCCURRENCE_ID).getFieldName(),
                                       OccurrenceSearchParameter.OCCURRENCE_ID, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.MEDIA_TYPE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.MEDIA_TYPE).getFieldName(),
                                       OccurrenceSearchParameter.MEDIA_TYPE, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.ESTABLISHMENT_MEANS).getFieldName(),
                                       OccurrenceSearchParameter.ESTABLISHMENT_MEANS, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.ISSUE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.ISSUE).getFieldName(),
                                       OccurrenceSearchParameter.ISSUE, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.INSTITUTION_CODE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.INSTITUTION_CODE).getFieldName(),
                                       OccurrenceSearchParameter.INSTITUTION_CODE, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.SCIENTIFIC_NAME,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.SCIENTIFIC_NAME).getFieldName(),
                                       OccurrenceSearchParameter.SCIENTIFIC_NAME, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.PROTOCOL,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.PROTOCOL).getFieldName(),
                                       OccurrenceSearchParameter.PROTOCOL, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.LICENSE,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.LICENSE).getFieldName(),
                                       OccurrenceSearchParameter.LICENSE, FacetField.Method.ENUM,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.CRAWL_ID,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.CRAWL_ID).getFieldName(),
                                       OccurrenceSearchParameter.CRAWL_ID, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.PUBLISHING_ORG,
           new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.PUBLISHING_ORG).getFieldName(),
                                       OccurrenceSearchParameter.PUBLISHING_ORG, FacetField.Method.FIELD_CACHE,
                                       FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.INSTALLATION_KEY,
        new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.INSTALLATION_KEY).getFieldName(),
          OccurrenceSearchParameter.INSTALLATION_KEY, FacetField.Method.FIELD_CACHE,
          FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.NETWORK_KEY,
        new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.NETWORK_KEY).getFieldName(),
          OccurrenceSearchParameter.NETWORK_KEY, FacetField.Method.FIELD_CACHE,
          FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.EVENT_ID,
        new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.EVENT_ID).getFieldName(),
          OccurrenceSearchParameter.EVENT_ID, FacetField.Method.FIELD_CACHE,
          FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.PARENT_EVENT_ID,
        new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.PARENT_EVENT_ID).getFieldName(),
          OccurrenceSearchParameter.PARENT_EVENT_ID, FacetField.Method.FIELD_CACHE,
          FacetField.SortOrder.COUNT, false))
      .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL,
        new FacetFieldConfiguration(QUERY_FIELD_MAPPING.get(OccurrenceSearchParameter.SAMPLING_PROTOCOL).getFieldName(),
          OccurrenceSearchParameter.SAMPLING_PROTOCOL, FacetField.Method.FIELD_CACHE,
          FacetField.SortOrder.COUNT, false))
      .build();


  }

  /**
   * Adds an occurrence date parameter: DATE or MODIFIED.
   */
  private static void addDateQuery(Multimap<OccurrenceSearchParameter, String> params,
                                   OccurrenceSearchParameter dateParam, OccurrenceSolrField solrField,
                                   SolrQuery solrQuery, boolean isFacetedSearch) {
    if (params.containsKey(dateParam)) {
      String dateParams = params.get(dateParam).stream()
        .map(value -> PARAMS_JOINER.join(solrField.getFieldName(), toDateQuery(value)))
        .collect(Collectors.joining(PARAM_OR_OP));
      solrQuery.addFilterQuery((isFacetedSearch ? taggedField(solrField.getFieldName()) : "")
                               + toParenthesesQuery(dateParams));
    }
  }

  /**
   * Add the occurrence bounding box and polygon parameters.
   * Those 2 parameters are returned in 1 filter expression because both refer to same Solr field: coordinate.
   */
  private static void addLocationQuery(Multimap<OccurrenceSearchParameter, String> params,
                                       SolrQuery solrQuery, boolean isFacetedSearch) {
    if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
      String locationParams = params.get(OccurrenceSearchParameter.GEOMETRY).stream()
        .map(value -> PARAMS_JOINER.join(OccurrenceSolrField.COORDINATE.getFieldName(),
                                         parseGeometryParam(value)))
        .collect(Collectors.joining(PARAM_OR_OP));
      solrQuery.addFilterQuery((isFacetedSearch ? taggedField(OccurrenceSolrField.COORDINATE.getFieldName()) : "")
                               + toParenthesesQuery(locationParams));
    }
  }

  /**
   * Adds the filter query to SolrQuery object.
   * Creates a conjunction of disjunctions: disjunctions(ORs) are created for the filter applied to the same field;
   * those disjunctions are joint in a big conjunction.
   */
  private static void setFilterParameters(OccurrenceSearchRequest request, SolrQuery solrQuery, boolean facetsEnable) {
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    boolean isFacetedSearch =  facetsEnable && request.getFacets() != null && !request.getFacets().isEmpty();
    if (params != null && !params.isEmpty()) {
      for (OccurrenceSearchParameter param : params.keySet()) {
        OccurrenceSolrField solrField = QUERY_FIELD_MAPPING.get(param);
        if (solrField != null) {
          Collection<String> paramValues = params.get(param);
          List<String> aFieldParameters = Lists.newArrayListWithExpectedSize(paramValues.size());
          for (String value : paramValues) {
            if (param.type() != Date.class) {
              String parsedValue = QueryUtils.parseQueryValue(value);
              if (QueryUtils.isRangeQuery(parsedValue)) {
                parsedValue = COMMON_REPLACER.matcher(parsedValue).replaceAll(" TO ");
              }
              if (Enum.class.isAssignableFrom(param.type())) { // enums are capitalized
                parsedValue = parsedValue.toUpperCase();
              }
              aFieldParameters.add(PARAMS_JOINER.join(solrField.getFieldName(), parsedValue));
            }
          }
          if (!aFieldParameters.isEmpty()) {
            solrQuery.addFilterQuery((isFacetedSearch ? taggedField(solrField.getFieldName()) : "")
                                     + toParenthesesQuery(PARAMS_OR_JOINER.join(aFieldParameters)));

          }
        }
      }
      addLocationQuery(params, solrQuery, isFacetedSearch);
      addDateQuery(params, OccurrenceSearchParameter.EVENT_DATE, OccurrenceSolrField.EVENT_DATE, solrQuery,
                   isFacetedSearch);
      addDateQuery(params, OccurrenceSearchParameter.LAST_INTERPRETED, OccurrenceSolrField.LAST_INTERPRETED, solrQuery,
                   isFacetedSearch);
    }
  }

  public boolean isFacetsEnable() {
    return facetsEnable;
  }

}
