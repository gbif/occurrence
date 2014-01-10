package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.common.search.util.QueryUtils;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
import static org.gbif.common.search.util.SolrConstants.GEO_INTERSECTS_QUERY_FMT;
import static org.gbif.common.search.util.SolrConstants.RANGE_FORMAT;
import static org.gbif.occurrence.search.OccurrenceSearchDateUtils.toDateQuery;


/**
 * Utility class for building Solr queries from supported parameters for occurrences search.
 */
public class OccurrenceSearchRequestBuilder {

  // This is a placeholder to map from the JSON definition ID to the query field
  public static final ImmutableMap<OccurrenceSearchParameter, OccurrenceSolrField> QUERY_FIELD_MAPPING =
    ImmutableMap.<OccurrenceSearchParameter, OccurrenceSolrField>builder()
      .put(OccurrenceSearchParameter.LATITUDE, OccurrenceSolrField.LATITUDE)
      .put(OccurrenceSearchParameter.LONGITUDE, OccurrenceSolrField.LONGITUDE)
      .put(OccurrenceSearchParameter.YEAR, OccurrenceSolrField.YEAR)
      .put(OccurrenceSearchParameter.MONTH, OccurrenceSolrField.MONTH)
      .put(OccurrenceSearchParameter.CATALOG_NUMBER, OccurrenceSolrField.CATALOG_NUMBER)
      .put(OccurrenceSearchParameter.COLLECTOR_NAME, OccurrenceSolrField.COLLECTOR_NAME)
      .put(OccurrenceSearchParameter.COLLECTION_CODE, OccurrenceSolrField.COLLECTION_CODE)
      .put(OccurrenceSearchParameter.INSTITUTION_CODE, OccurrenceSolrField.INSTITUTION_CODE)
      .put(OccurrenceSearchParameter.DEPTH, OccurrenceSolrField.DEPTH)
      .put(OccurrenceSearchParameter.ALTITUDE, OccurrenceSolrField.ALTITUDE)
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD, OccurrenceSolrField.BASIS_OF_RECORD)
      .put(OccurrenceSearchParameter.DATASET_KEY, OccurrenceSolrField.DATASET_KEY)
      .put(OccurrenceSearchParameter.SPATIAL_ISSUES, OccurrenceSolrField.GEOSPATIAL_ISSUE)
      .put(OccurrenceSearchParameter.GEOREFERENCED, OccurrenceSolrField.GEOREFERENCED)
      .put(OccurrenceSearchParameter.DATE, OccurrenceSolrField.DATE)
      .put(OccurrenceSearchParameter.MODIFIED, OccurrenceSolrField.MODIFIED)
      .put(OccurrenceSearchParameter.COUNTRY, OccurrenceSolrField.COUNTRY)
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, OccurrenceSolrField.PUBLISHING_COUNTRY)
      .put(OccurrenceSearchParameter.TAXON_KEY, OccurrenceSolrField.TAXON_KEY)
      .build();

  private static final BasisOfRecordConverter BASIS_OF_RECORD_CONVERTER = new BasisOfRecordConverter();


  // Holds the value used for an optional sort order applied to a search via param "sort"
  private final Map<String, SolrQuery.ORDER> sortOrder;

  // Solr request handler.
  private final String requestHandler;

  public final static int MAX_OFFSET = 1000000;
  public final static int MAX_PAGE_SIZE = 300;

  /**
   * Default constructor.
   */
  public OccurrenceSearchRequestBuilder(String requestHandler, Map<String, SolrQuery.ORDER> sortOrder) {
    this.requestHandler = requestHandler;
    this.sortOrder = sortOrder;
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
      return String.format(GEO_INTERSECTS_QUERY_FMT, wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }


  public SolrQuery build(@Nullable OccurrenceSearchRequest request) {
    final int maxOffset = MAX_OFFSET - request.getLimit();
    Preconditions.checkArgument(request.getOffset() <= maxOffset, "maximum offset allowed is %s", MAX_OFFSET);

    SolrQuery solrQuery = new SolrQuery();
    // q param
    solrQuery.setQuery(DEFAULT_QUERY);
    // paging
    setQueryPaging(request, solrQuery, MAX_PAGE_SIZE);
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
  private void addDateQuery(Multimap<OccurrenceSearchParameter, String> params,
    OccurrenceSearchParameter dateParam, OccurrenceSolrField solrField, List<String> filterQueries) {
    if (params.containsKey(dateParam)) {
      List<String> dateParams = new ArrayList<String>();
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
  private void addLocationQuery(Multimap<OccurrenceSearchParameter, String> params, List<String> filterQueries) {
    if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
      List<String> locationParams = new ArrayList<String>();
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
  private void setFilterParameters(OccurrenceSearchRequest request, SolrQuery solrQuery) {
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    if (params != null && !params.isEmpty()) {
      List<String> filterQueries = Lists.newArrayList();
      for (OccurrenceSearchParameter param : params.keySet()) {
        List<String> aFieldParameters = Lists.newArrayList();
        for (String value : params.get(param)) {
          SearchTypeValidator.validate(param, value);
          OccurrenceSolrField solrField = QUERY_FIELD_MAPPING.get(param);
          if (solrField != null && param.type() != Date.class) {
            String parsedValue;
            if (solrField == OccurrenceSolrField.BASIS_OF_RECORD) {
              parsedValue = BASIS_OF_RECORD_CONVERTER.fromEnum(BasisOfRecord.valueOf(value)).toString();
            } else {
              parsedValue = QueryUtils.parseQueryValue(value);
            }
            if (QueryUtils.isRangeQuery(parsedValue)) {
              parsedValue = parsedValue.replace(",", " TO ");
            }
            aFieldParameters.add(PARAMS_JOINER.join(solrField.getFieldName(), parsedValue));
          }
        }
        if (!aFieldParameters.isEmpty()) {
          filterQueries.add(toParenthesesQuery(PARAMS_OR_JOINER.join(aFieldParameters)));
        }
      }
      addLocationQuery(params, filterQueries);
      addDateQuery(params, OccurrenceSearchParameter.DATE, OccurrenceSolrField.DATE, filterQueries);
      addDateQuery(params, OccurrenceSearchParameter.MODIFIED, OccurrenceSolrField.MODIFIED, filterQueries);

      if (!filterQueries.isEmpty()) {
        solrQuery.addFilterQuery(PARAMS_AND_JOINER.join(filterQueries));
      }
    }
  }
}
