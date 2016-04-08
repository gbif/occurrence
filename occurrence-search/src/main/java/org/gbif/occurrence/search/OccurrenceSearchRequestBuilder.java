package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.common.search.util.QueryUtils;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.solr.client.solrj.SolrQuery;

import static org.gbif.common.search.util.QueryUtils.PARAMS_AND_JOINER;
import static org.gbif.common.search.util.QueryUtils.PARAMS_JOINER;
import static org.gbif.common.search.util.QueryUtils.PARAMS_OR_JOINER;
import static org.gbif.common.search.util.QueryUtils.setQueryPaging;
import static org.gbif.common.search.util.QueryUtils.setRequestHandler;
import static org.gbif.common.search.util.QueryUtils.setSortOrder;
import static org.gbif.common.search.util.QueryUtils.toParenthesesQuery;
import static org.gbif.common.search.util.SolrConstants.DEFAULT_QUERY;
import static org.gbif.common.search.util.SolrConstants.RANGE_FORMAT;
import static org.gbif.occurrence.search.OccurrenceSearchDateUtils.toDateQuery;


/**
 * Utility class for building Solr queries from supported parameters for occurrences search.
 */
public class OccurrenceSearchRequestBuilder {

  private static final String SOLR_SPELLCHECK = "spellcheck";
  private static final String SOLR_SPELLCHECK_COUNT = "spellcheck.count";
  private static final Integer DEFAULT_SPELL_CHECK_COUNT = 4;

  /**
   * Utility class to generates full text queries.
   */
  private static final class OccurrenceFullTextQueryBuilder {


    private String q;

    private static final Double FUZZY_DISTANCE = 0.7;

    private static final String TERM_PATTERN = "%1$s^%2$s %1$s~%3$s^%4$s";

    private static final String NON_TOKENIZED_QUERY_PATTERN = ":%1$s^1000";

    private static final Set<String> NON_TOKENIZABLE_FIELDS =
      ImmutableSet.<String>of(OccurrenceSolrField.CATALOG_NUMBER.getFieldName() + NON_TOKENIZED_QUERY_PATTERN,
                              OccurrenceSolrField.OCCURRENCE_ID.getFieldName() + NON_TOKENIZED_QUERY_PATTERN);


    private static final String NON_TOKENIZED_QUERY = QueryUtils.PARAMS_OR_JOINER.join(NON_TOKENIZABLE_FIELDS);

    private static final Integer MAX_SCORE = 100;

    private static final Integer SCORE_DECREMENT = 20;



    /**
     * Query parameter.
     */
    private OccurrenceFullTextQueryBuilder withQ(String q){
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
      String unTokenizedFieldsQuery = String.format(NON_TOKENIZED_QUERY, q);
      if(qs.length > 1){
        StringBuilder ftQ = new StringBuilder();
        ftQ.append(QueryUtils.toPhraseQuery(q) +  ' ');
        for(int i = 0; i < qs.length; i++) {
          int termScore = Math.max(MAX_SCORE - SCORE_DECREMENT * i, SCORE_DECREMENT);
          ftQ.append(String.format(TERM_PATTERN, qs[i], termScore, FUZZY_DISTANCE, termScore / 2));
          if (i < qs.length - 1) {
            ftQ.append(' ');
          }
        }
        return QueryUtils.PARAMS_OR_JOINER.join(unTokenizedFieldsQuery, ftQ.toString());
      }
      return  QueryUtils.PARAMS_OR_JOINER.join(unTokenizedFieldsQuery,
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
      .put(OccurrenceSearchParameter.TYPE_STATUS, OccurrenceSolrField.TYPE_STATUS)
      .put(OccurrenceSearchParameter.MEDIA_TYPE, OccurrenceSolrField.MEDIA_TYPE)
      .put(OccurrenceSearchParameter.ISSUE, OccurrenceSolrField.ISSUE)
      .put(OccurrenceSearchParameter.OCCURRENCE_ID, OccurrenceSolrField.OCCURRENCE_ID)
      .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, OccurrenceSolrField.ESTABLISHMENT_MEANS)
      .build();

  public static final String GEO_INTERSECTS_QUERY_FMT = "\"IsWithin(%s) distErrPct=0\"";

  // Solr full text search handle
  private static final String FULL_TEXT_HANDLER = "/search";

  // Holds the value used for an optional sort order applied to a search via param "sort"
  private final Map<String, SolrQuery.ORDER> sortOrder;

  // Solr request handler.
  private final String requestHandler;

  private final int maxOffset;

  private final int maxLimit;

  public static final int MAX_OFFSET = 1000000;
  public static final int MAX_PAGE_SIZE = 300;

  /**
   * Default constructor.
   */
  public OccurrenceSearchRequestBuilder(String requestHandler, Map<String, SolrQuery.ORDER> sortOrder, int maxOffset, int maxLimit) {
    Preconditions.checkArgument(maxOffset > 0, "Max offset can't less than zero");
    Preconditions.checkArgument(maxLimit > 0, "Max limit can't less than zero");
    this.requestHandler = requestHandler;
    this.sortOrder = sortOrder;
    this.maxOffset = Math.min(maxOffset,MAX_OFFSET);
    this.maxLimit = Math.min(maxLimit,MAX_PAGE_SIZE);
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
      if (geometry.isRectangle()) {
        Envelope bbox = geometry.getEnvelopeInternal();
        return String
          .format(RANGE_FORMAT, bbox.getMinY() + "," + bbox.getMinX(), bbox.getMaxY() + "," + bbox.getMaxX());
      }
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
    if(request.isSpellCheck()){
      solrQuery.setParam(SOLR_SPELLCHECK_COUNT,
                         request.getSpellCheckCount() < 0 ? DEFAULT_SPELL_CHECK_COUNT.toString() : Integer.toString(request.getSpellCheckCount()));
    }
    // q param
    if(Strings.isNullOrEmpty(request.getQ())) {
      solrQuery.setQuery(DEFAULT_QUERY);
    } else {
      OccurrenceFullTextQueryBuilder occurrenceFullTextQueryBuilder = new OccurrenceFullTextQueryBuilder();
      occurrenceFullTextQueryBuilder.withQ(request.getQ());
      solrQuery.setQuery(occurrenceFullTextQueryBuilder.build());
    }
    solrQuery.setRequestHandler(FULL_TEXT_HANDLER);
    // paging
    setQueryPaging(request, solrQuery, maxLimit);
    // sets the filters
    setFilterParameters(request, solrQuery);
    // sorting
    setSortOrder(solrQuery, sortOrder);
    // set the request handler
    setRequestHandler(solrQuery, requestHandler);

    return solrQuery;
  }

  /**
   * Adds an occurrence date parameter: DATE or MODIFIED.
   */
  private static void addDateQuery(Multimap<OccurrenceSearchParameter, String> params,
    OccurrenceSearchParameter dateParam, OccurrenceSolrField solrField, List<String> filterQueries) {
    if (params.containsKey(dateParam)) {
      Collection<String> dateParams = new ArrayList<String>();
      for (String value : params.get(dateParam)) {
        dateParams.add(PARAMS_JOINER.join(solrField.getFieldName(), toDateQuery(value)));
      }
      filterQueries.add(toParenthesesQuery(PARAMS_OR_JOINER.join(dateParams)));
    }
  }

  /**
   * Add the occurrence bounding box and polygon parameters.
   * Those 2 parameters are returned in 1 filter expression because both refer to same Solr field: coordinate.
   */
  private static void addLocationQuery(Multimap<OccurrenceSearchParameter,String> params,
                                       Collection<String> filterQueries) {
    if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
      Collection<String> locationParams = new ArrayList<String>();
      for (String value : params.get(OccurrenceSearchParameter.GEOMETRY)) {
        locationParams
          .add(PARAMS_JOINER.join(OccurrenceSolrField.COORDINATE.getFieldName(), parseGeometryParam(value)));
      }
      filterQueries.add(toParenthesesQuery(PARAMS_OR_JOINER.join(locationParams)));
    }
  }

  /**
   * Adds the filter query to SolrQuery object.
   * Creates a conjunction of disjunctions: disjunctions(ORs) are created for the filter applied to the same field;
   * The those disjunctions are joint in a big conjunction.
   */
  private static void setFilterParameters(OccurrenceSearchRequest request, SolrQuery solrQuery) {
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    if (params != null && !params.isEmpty()) {
      List<String> filterQueries = Lists.newArrayList();
      for (OccurrenceSearchParameter param : params.keySet()) {
        List<String> aFieldParameters = Lists.newArrayList();
        for (String value : params.get(param)) {
          OccurrenceSolrField solrField = QUERY_FIELD_MAPPING.get(param);
          if (solrField != null && param.type() != Date.class) {
            String parsedValue = QueryUtils.parseQueryValue(value);
            if (QueryUtils.isRangeQuery(parsedValue)) {
              parsedValue = parsedValue.replace(",", " TO ");
            }
            if (Enum.class.isAssignableFrom(param.type())) { // enums are capitalized
              parsedValue = parsedValue.toUpperCase();
            }
            aFieldParameters.add(PARAMS_JOINER.join(solrField.getFieldName(), parsedValue));
          }
        }
        if (!aFieldParameters.isEmpty()) {
          filterQueries.add(toParenthesesQuery(PARAMS_OR_JOINER.join(aFieldParameters)));
        }
      }
      addLocationQuery(params, filterQueries);
      addDateQuery(params, OccurrenceSearchParameter.EVENT_DATE, OccurrenceSolrField.EVENT_DATE, filterQueries);
      addDateQuery(params, OccurrenceSearchParameter.LAST_INTERPRETED, OccurrenceSolrField.LAST_INTERPRETED,
        filterQueries);

      if (!filterQueries.isEmpty()) {
        solrQuery.addFilterQuery(PARAMS_AND_JOINER.join(filterQueries));
      }
    }
  }
}
