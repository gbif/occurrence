package org.gbif.occurrence.search.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;
import static org.junit.Assert.*;

public class OccurrenceEsSearchRequestBuilderTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OccurrenceEsSearchRequestBuilderTest.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INDEX = "index";

  @Test
  public void termQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addKingdomKeyFilter(6);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    assertEquals(1, jsonQuery.path(BOOL).path(FILTER).size());
    assertEquals(
        6,
        jsonQuery.path(BOOL).path(FILTER).findValue(KINGDOM_KEY.getFieldName()).get(VALUE).asInt());
  }

  @Test
  public void multiTermQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addYearFilter(1999);
    searchRequest.addCountryFilter(Country.AFGHANISTAN);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    assertEquals(2, jsonQuery.path(BOOL).path(FILTER).size());
    assertEquals(
        1999, jsonQuery.path(BOOL).path(FILTER).findValue(YEAR.getFieldName()).get(VALUE).asInt());
    assertEquals(
        Country.AFGHANISTAN.getIso2LetterCode(),
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValue(COUNTRY_CODE.getFieldName())
            .get(VALUE)
            .asText());
  }

  @Test
  public void multivalueTermQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addMonthFilter(1);
    searchRequest.addMonthFilter(2);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    assertTrue(jsonQuery.path(BOOL).path(FILTER).get(0).has(TERMS));
    assertEquals(
        2, jsonQuery.path(BOOL).path(FILTER).get(0).path(TERMS).path(MONTH.getFieldName()).size());
  }

  @Test
  public void rangeQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LATITUDE, "12, 25");

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    JsonNode latitudeNode =
        jsonQuery.path(BOOL).path(FILTER).findValue(RANGE).path(LATITUDE.getFieldName());
    assertEquals(12, latitudeNode.path(FROM).asDouble(), 0);
    assertEquals(25, latitudeNode.path(TO).asDouble(), 0);
  }

  @Test
  public void polygonQueryTest() throws IOException {
    final String polygon = "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(polygon);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("polygon", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(5, shape.get(COORDINATES).get(0).size());
  }

  @Test
  public void polygonWithHoleQueryTest() throws IOException {
    final String polygonWithHole =
        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(polygonWithHole);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("polygon", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(5, shape.get(COORDINATES).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(1).size());
  }

  @Test
  public void multipolygonQueryTest() throws IOException {
    final String multipolygon =
        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(multipolygon);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("multipolygon", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(1, shape.get(COORDINATES).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(0).get(0).size());
    assertEquals(2, shape.get(COORDINATES).get(1).size());
    assertEquals(5, shape.get(COORDINATES).get(1).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(1).get(1).size());
  }

  @Test
  public void linestringQueryTest() throws IOException {
    final String linestring = "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(linestring);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("linestring", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(-77.03653, shape.get(COORDINATES).get(0).get(0).asDouble(), 0);
  }

  @Test
  public void linearringQueryTest() throws IOException {
    final String linearring = "LINEARRING (12 12, 14 10, 13 14, 12 12)";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(linearring);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("linestring", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(4, shape.get(COORDINATES).size());
    assertEquals(12, shape.get(COORDINATES).get(0).get(0).asDouble(), 0);
  }

  @Test
  public void pointQueryTest() throws IOException {
    final String point = "POINT (-77.03653 38.897676)";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(point);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("point", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(-77.03653d, shape.get(COORDINATES).get(0).asDouble(), 0);
  }

  @Test
  public void simpleFacetQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addFacets(OccurrenceSearchParameter.BASIS_OF_RECORD);
    searchRequest.addFacetPage(OccurrenceSearchParameter.BASIS_OF_RECORD, 0, 5);

    SearchRequest request =
        EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 0, 0, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(AGGREGATIONS).has(BASIS_OF_RECORD.getFieldName()));
    assertFalse(jsonQuery.has(POST_FILTER));

    JsonNode aggs = jsonQuery.path(AGGREGATIONS).path(BASIS_OF_RECORD.getFieldName());
    assertEquals(BASIS_OF_RECORD.getFieldName(), aggs.path(TERMS).path(FIELD).asText());
    assertEquals(5, aggs.path(TERMS).path(SIZE).asInt());
  }

  @Test
  public void simpleFacetWithParamsQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addFacets(OccurrenceSearchParameter.BASIS_OF_RECORD);
    searchRequest.addFacetPage(OccurrenceSearchParameter.BASIS_OF_RECORD, 0, 5);
    searchRequest.addMonthFilter(1);

    SearchRequest request =
        EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 0, 0, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(QUERY).path(BOOL).has(FILTER));
    JsonNode queryFilter = jsonQuery.path(QUERY).path(BOOL).path(FILTER);
    assertEquals(1, queryFilter.get(0).path(TERM).path(MONTH.getFieldName()).path(VALUE).asInt());

    assertTrue(jsonQuery.path(AGGREGATIONS).has(BASIS_OF_RECORD.getFieldName()));
  }

  @Test
  public void multiselectFacetQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addFacets(OccurrenceSearchParameter.BASIS_OF_RECORD);
    searchRequest.addFacetPage(OccurrenceSearchParameter.BASIS_OF_RECORD, 0, 5);
    searchRequest.addBasisOfRecordFilter(BasisOfRecord.PRESERVED_SPECIMEN);
    searchRequest.setMultiSelectFacets(true);

    SearchRequest request =
        EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 0, 0, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    // assert aggs
    assertTrue(jsonQuery.path(AGGREGATIONS).has(BASIS_OF_RECORD.getFieldName()));
    JsonNode aggs = jsonQuery.path(AGGREGATIONS).path(BASIS_OF_RECORD.getFieldName());
    assertEquals(BASIS_OF_RECORD.getFieldName(), aggs.path(TERMS).path(FIELD).asText());
    assertEquals(5, aggs.path(TERMS).path(SIZE).asInt());

    // assert post filter
    assertTrue(jsonQuery.has(POST_FILTER));
    JsonNode postFilter =
        jsonQuery
            .path(POST_FILTER)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERM)
            .path(BASIS_OF_RECORD.getFieldName());
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN.name(), postFilter.path(VALUE).asText());
  }

  @Test
  public void multiselectMultipleFacetsQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addFacets(OccurrenceSearchParameter.BASIS_OF_RECORD);
    searchRequest.addFacetPage(OccurrenceSearchParameter.BASIS_OF_RECORD, 0, 5);
    searchRequest.addBasisOfRecordFilter(BasisOfRecord.PRESERVED_SPECIMEN);
    searchRequest.addFacets(OccurrenceSearchParameter.MONTH);
    searchRequest.addFacetPage(OccurrenceSearchParameter.MONTH, 0, 6);
    searchRequest.addMonthFilter(1);
    searchRequest.addYearFilter(1999);
    searchRequest.setMultiSelectFacets(true);

    SearchRequest request =
        EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 0, 0, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    // assert query
    assertEquals(
        1999,
        jsonQuery
            .path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERM)
            .path(YEAR.getFieldName())
            .get(VALUE)
            .asInt());

    // assert aggs basis of record
    assertTrue(jsonQuery.path(AGGREGATIONS).has(BASIS_OF_RECORD.getFieldName()));
    JsonNode basisOfRecordAggs = jsonQuery.path(AGGREGATIONS).path(BASIS_OF_RECORD.getFieldName());
    assertEquals(
        1,
        basisOfRecordAggs
            .path(FILTER)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERM)
            .path(MONTH.getFieldName())
            .path(VALUE)
            .asInt());

    assertTrue(
        basisOfRecordAggs.path(AGGREGATIONS).has("filtered_" + BASIS_OF_RECORD.getFieldName()));
    assertEquals(
        BASIS_OF_RECORD.getFieldName(),
        basisOfRecordAggs
            .path(AGGREGATIONS)
            .path("filtered_" + BASIS_OF_RECORD.getFieldName())
            .path(TERMS)
            .path(FIELD)
            .asText());

    // assert aggs month
    assertTrue(jsonQuery.path(AGGREGATIONS).has(MONTH.getFieldName()));
    JsonNode monthAggs = jsonQuery.path(AGGREGATIONS).path(MONTH.getFieldName());
    assertEquals(
        BasisOfRecord.PRESERVED_SPECIMEN.name(),
        monthAggs
            .path(FILTER)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERM)
            .path(BASIS_OF_RECORD.getFieldName())
            .path(VALUE)
            .asText());

    assertTrue(monthAggs.path(AGGREGATIONS).has("filtered_" + MONTH.getFieldName()));
    assertEquals(
        MONTH.getFieldName(),
        monthAggs
            .path(AGGREGATIONS)
            .path("filtered_" + MONTH.getFieldName())
            .path(TERMS)
            .path(FIELD)
            .asText());

    // assert post filter
    assertEquals(2, jsonQuery.path(POST_FILTER).path(BOOL).path(FILTER).size());
    JsonNode postFilter = jsonQuery.path(POST_FILTER).path(BOOL).path(FILTER);
    assertEquals(
        BasisOfRecord.PRESERVED_SPECIMEN.name(),
        postFilter.findValue(BASIS_OF_RECORD.getFieldName()).path(VALUE).asText());
    assertEquals(1, postFilter.findValue(MONTH.getFieldName()).path(VALUE).asInt());
  }

  @Test
  public void sourceParametersTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.setLimit(10);
    searchRequest.setOffset(2);

    SearchRequest request =
        EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 200, 20, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    assertEquals(2, jsonQuery.path(FROM).asInt());
    assertEquals(10, jsonQuery.path(SIZE).asInt());

    request = EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 1, 2, INDEX);
    jsonQuery = MAPPER.readTree(request.source().toString());

    assertEquals(1, jsonQuery.path(FROM).asInt());
    assertEquals(2, jsonQuery.path(SIZE).asInt());
  }

  @Test
  public void groupParametersTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addYearFilter(1999);
    searchRequest.addMonthFilter(2);

    EsSearchRequestBuilder.GroupedParams groupedParams =
        EsSearchRequestBuilder.groupParameters(searchRequest);

    // only parameters
    assertEquals(2, groupedParams.queryParams.size());
    assertNull(groupedParams.postFilterParams);

    // facets
    searchRequest.addFacets(OccurrenceSearchParameter.YEAR);
    groupedParams = EsSearchRequestBuilder.groupParameters(searchRequest);
    assertEquals(2, groupedParams.queryParams.size());
    assertNull(groupedParams.postFilterParams);

    // multiselect
    searchRequest.setMultiSelectFacets(true);
    groupedParams = EsSearchRequestBuilder.groupParameters(searchRequest);
    assertEquals(1, groupedParams.queryParams.size());
    assertEquals(1, groupedParams.postFilterParams.size());

    searchRequest.addParameter(OccurrenceSearchParameter.KINGDOM_KEY, 4);
    searchRequest.addParameter(OccurrenceSearchParameter.KINGDOM_KEY, 6);
    groupedParams = EsSearchRequestBuilder.groupParameters(searchRequest);
    assertEquals(2, groupedParams.queryParams.keySet().size());
    assertEquals(3, groupedParams.queryParams.values().size());
    assertEquals(2, groupedParams.queryParams.get(OccurrenceSearchParameter.KINGDOM_KEY).size());
  }

  @Test
  public void matchQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.setQ("puma");

    SearchRequest request =
        EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 200, 20, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    JsonNode matchNode = jsonQuery.path(QUERY).path(BOOL).path(MUST).get(0).path(MATCH);
    assertTrue(matchNode.has(_ALL));
    assertEquals("puma", matchNode.path(_ALL).path(QUERY).asText());
  }

  @Test
  public void sortQueryTest() throws IOException {
    // sort by score desc for q param
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.setQ("puma");

    SearchRequest request =
      EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 200, 20, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);
    assertEquals("desc", jsonQuery.path("sort").get(0).path("_score").path("order").asText());

    // mix with q param and term
    searchRequest.addMonthFilter(1);
    request =
      EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 200, 20, INDEX);
    jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);
    assertEquals("desc", jsonQuery.path("sort").get(0).path("_score").path("order").asText());

    // sort by _doc desc if q param is not present
    searchRequest = new OccurrenceSearchRequest();
    searchRequest.addKingdomKeyFilter(4);

    request =
      EsSearchRequestBuilder.buildSearchRequest(searchRequest, true, 200, 20, INDEX);
    jsonQuery = MAPPER.readTree(request.source().toString());
    LOG.debug("Query: {}", jsonQuery);

    assertEquals("desc", jsonQuery.path("sort").get(0).path("_doc").path("order").asText());
  }

}
