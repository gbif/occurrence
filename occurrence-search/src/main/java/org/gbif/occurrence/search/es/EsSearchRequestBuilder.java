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
package org.gbif.occurrence.search.es;

import static org.gbif.api.util.SearchTypeValidator.isNumericRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.CARDINALITIES;
import static org.gbif.occurrence.search.es.EsQueryUtils.RANGE_SEPARATOR;
import static org.gbif.occurrence.search.es.EsQueryUtils.RANGE_WILDCARD;
import static org.gbif.occurrence.search.es.EsQueryUtils.extractFacetLimit;
import static org.gbif.occurrence.search.es.EsQueryUtils.extractFacetOffset;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.gbif.api.model.common.search.SearchConstants;
import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.search.OccurrencePredicateSearchRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.Range;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.search.configuration.NameUsageMatchServiceTriage;
import org.gbif.occurrence.search.predicate.QueryVisitorFactory;
import org.gbif.predicate.query.EsQueryVisitor;
import org.gbif.vocabulary.client.ConceptClient;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.util.StringUtils;

public class EsSearchRequestBuilder {

  private static final int MAX_SIZE_TERMS_AGGS = 1200000;
  private static final IntUnaryOperator DEFAULT_SHARD_SIZE = size -> (size * 2) + 50000;

  public static String[] SOURCE_EXCLUDE =
      new String[] {"all", "notIssues", "*.verbatim", "*.suggest"};

  private final OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper;
  private final ConceptClient conceptClient;
  private final NameUsageMatchServiceTriage triage;

  public EsSearchRequestBuilder(
      OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper, ConceptClient conceptClient, NameUsageMatchServiceTriage triage) {
    this.occurrenceBaseEsFieldMapper = occurrenceBaseEsFieldMapper;
    this.conceptClient = conceptClient;
    this.triage = triage;
  }

  public SearchRequest buildSearchRequest(OccurrenceSearchRequest searchRequest, String index) {

    SearchRequest esRequest = new SearchRequest();
    esRequest.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    esRequest.source(searchSourceBuilder);

    // size and offset
    searchSourceBuilder.size(searchRequest.getLimit());
    searchSourceBuilder.from((int) searchRequest.getOffset());
    searchSourceBuilder.trackTotalHits(true);
    searchSourceBuilder.fetchSource(null, SOURCE_EXCLUDE);

    // group params
    GroupedParams groupedParams = groupParameters(searchRequest);

    // add query
    if (searchRequest instanceof OccurrencePredicateSearchRequest) {
      buildQuery((OccurrencePredicateSearchRequest) searchRequest)
          .ifPresent(searchSourceBuilder::query);
    } else {
      buildQuery(groupedParams.queryParams, searchRequest.getQ(), searchRequest.isMatchCase())
          .ifPresent(searchSourceBuilder::query);
    }

    // sort
    if (!Strings.isNullOrEmpty(searchRequest.getShuffle())) {
      // random score
      searchSourceBuilder.sort(
          SortBuilders.scriptSort(
              new Script(
                  ScriptType.INLINE,
                  "painless",
                  "(doc['_id'].value + params['seed']).hashCode()",
                  Collections.singletonMap("seed", searchRequest.getShuffle())),
              ScriptSortBuilder.ScriptSortType.NUMBER));
    } else if (Strings.isNullOrEmpty(searchRequest.getQ())) {
      occurrenceBaseEsFieldMapper.getDefaultSort().forEach(searchSourceBuilder::sort);
    } else {
      searchSourceBuilder.sort(SortBuilders.scoreSort());
    }

    // add aggs
    buildAggs(searchRequest, groupedParams.postFilterParams)
        .ifPresent(aggsList -> aggsList.forEach(searchSourceBuilder::aggregation));

    // post-filter
    buildPostFilter(groupedParams.postFilterParams, searchRequest.isMatchCase())
        .ifPresent(searchSourceBuilder::postFilter);

    return esRequest;
  }

  public Optional<QueryBuilder> buildQueryNode(OccurrenceSearchRequest searchRequest) {
    return buildQuery(
        searchRequest.getParameters(), searchRequest.getQ(), searchRequest.isMatchCase());
  }

  SearchRequest buildSuggestQuery(
      String prefix, OccurrenceSearchParameter parameter, Integer limit, String index) {
    SearchRequest request = new SearchRequest();
    request.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    request.source(searchSourceBuilder);

    EsField esField = occurrenceBaseEsFieldMapper.getEsField(parameter);

    // create suggest query
    searchSourceBuilder.suggest(
        new SuggestBuilder()
            .addSuggestion(
                esField.getSearchFieldName(),
                SuggestBuilders.completionSuggestion(esField.getSuggestFieldName())
                    .prefix(prefix)
                    .size(limit != null ? limit : SearchConstants.DEFAULT_SUGGEST_LIMIT)
                    .skipDuplicates(true)));

    // add source field
    searchSourceBuilder.fetchSource(esField.getSearchFieldName(), null);

    return request;
  }

  // Method to build and add a nested checklist query
  private void addChecklistKeyTaxonKeyQuery(Map<OccurrenceSearchParameter, Set<String>> params,
                                            BoolQueryBuilder bool,
                                            OccurrenceSearchParameter taxonParam,
                                            String taxonField) {
    if (params.containsKey(OccurrenceSearchParameter.CHECKLIST_KEY) && params.containsKey(taxonParam)) {
      // Build the query
      BoolQueryBuilder checklistQuery = QueryBuilders.boolQuery()
        .must(QueryBuilders.termQuery(taxonField, params.get(taxonParam).iterator().next()));

      // Create a NestedQueryBuilder to search within the "classifications" nested object
      params.remove(taxonParam);
      bool.filter().add(checklistQuery);
    }
  }

  // Method to build and add a nested checklist query
  private void addChecklistKeyTaxonKeyQuery(
      String checklistKey,
      String rank,
      BoolQueryBuilder bool,
      Map<OccurrenceSearchParameter, Set<String>> params) {

    OccurrenceSearchParameter taxonParam = new OccurrenceSearchParameter(rank.toUpperCase() + "_KEY", String.class);
    if (!StringUtils.isEmpty(checklistKey) && params.containsKey(taxonParam)) {
      BoolQueryBuilder checklistQuery = QueryBuilders.boolQuery()
        .must(QueryBuilders.termQuery("classifications."
          + checklistKey + ".taxonKeys.keyword", params.get(taxonParam).iterator().next()));
      params.remove(taxonParam);
      bool.filter().add(checklistQuery);
    }
  }

  @VisibleForTesting
  Optional<QueryBuilder> buildQuery(
      Map<OccurrenceSearchParameter, Set<String>> params, String qParam, boolean matchCase) {
    VocabularyFieldTranslator.translateVocabs(params, conceptClient);

    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(occurrenceBaseEsFieldMapper.getFullTextField(), qParam));
    }

    if (params != null && !params.isEmpty()) {

      // Add the queries based on different taxonomic levels
      handleTaxonomicQueries(params, bool);

      // adding geometry to bool
      if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
        BoolQueryBuilder shouldGeometry = QueryBuilders.boolQuery();
        shouldGeometry
            .should()
            .addAll(
                params.get(OccurrenceSearchParameter.GEOMETRY).stream()
                    .map(this::buildGeoShapeQuery)
                    .collect(Collectors.toList()));
        bool.filter().add(shouldGeometry);
      }

      if (params.containsKey(OccurrenceSearchParameter.GEO_DISTANCE)) {
        BoolQueryBuilder shouldGeoDistance = QueryBuilders.boolQuery();
        shouldGeoDistance
            .should()
            .addAll(
                params.get(OccurrenceSearchParameter.GEO_DISTANCE).stream()
                    .map(this::buildGeoDistanceQuery)
                    .collect(Collectors.toList()));
        bool.filter().add(shouldGeoDistance);
      }

      // adding term queries to bool
      bool.filter()
          .addAll(
              params.entrySet().stream()
                  .filter(e -> Objects.nonNull(occurrenceBaseEsFieldMapper.getEsField(e.getKey())))
                  .flatMap(
                      e ->
                          buildTermQuery(
                              e.getValue(),
                              e.getKey(),
                              occurrenceBaseEsFieldMapper.getEsField(e.getKey()),
                              matchCase)
                              .stream())
                  .collect(Collectors.toList()));
    }
    occurrenceBaseEsFieldMapper.getDefaultFilter().ifPresent(df -> bool.filter().add(df));

    // bool.filter().add(QueryBuilders.termQuery("type",
    // esFieldMapper.getSearchType().getObjectName()));
    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  private void handleTaxonomicQueries(Map<OccurrenceSearchParameter, Set<String>> params, BoolQueryBuilder bool) {

    if (params.containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)) {
      String checklistKey = params.get(OccurrenceSearchParameter.CHECKLIST_KEY).iterator().next();

      // FIXME taxon key queries - check if we need to search
      addChecklistKeyTaxonKeyQuery(params, bool, OccurrenceSearchParameter.TAXON_KEY,
        "classifications." + checklistKey + ".taxonKeys.keyword");

      addChecklistKeyTaxonKeyQuery(params, bool, OccurrenceSearchParameter.ACCEPTED_TAXON_KEY,
        "classifications." + checklistKey + ".acceptedUsage.key.keyword");

      triage.getChecklistRanks(checklistKey).forEach(rank -> {
        addChecklistKeyTaxonKeyQuery(
          checklistKey,
          rank,
          bool,
          params
        );
      });
    }
  }

  @SneakyThrows
  public Optional<BoolQueryBuilder> buildQuery(OccurrencePredicateSearchRequest searchRequest) {
    VocabularyFieldTranslator.translateVocabs(searchRequest, conceptClient);

    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    String qParam = searchRequest.getQ();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(occurrenceBaseEsFieldMapper.getFullTextField(), qParam));
    }

    EsQueryVisitor<OccurrenceSearchParameter> esQueryVisitor =
      QueryVisitorFactory.createEsQueryVisitor(occurrenceBaseEsFieldMapper);
    esQueryVisitor.getQueryBuilder(searchRequest.getPredicate()).ifPresent(bool::must);

    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  @VisibleForTesting
  static GroupedParams groupParameters(OccurrenceSearchRequest searchRequest) {
    GroupedParams groupedParams = new GroupedParams();

    if (!searchRequest.isMultiSelectFacets()
        || searchRequest.getFacets() == null
        || searchRequest.getFacets().isEmpty()) {
      groupedParams.queryParams = searchRequest.getParameters();
      return groupedParams;
    }

    groupedParams.queryParams = new HashMap<>();
    groupedParams.postFilterParams = new HashMap<>();

    searchRequest
        .getParameters()
        .forEach(
            (k, v) -> {
              if (searchRequest.getFacets().contains(k)) {
                groupedParams.postFilterParams.put(k, v);
              } else {
                groupedParams.queryParams.put(k, v);
              }
            });

    return groupParameters(searchRequest, searchRequest.isMultiSelectFacets());
  }

  static GroupedParams groupParameters(
      OccurrenceSearchRequest searchRequest, boolean groupFilters) {
    GroupedParams groupedParams = new GroupedParams();

    if (!groupFilters || searchRequest.getFacets() == null || searchRequest.getFacets().isEmpty()) {
      groupedParams.queryParams = searchRequest.getParameters();
      return groupedParams;
    }

    groupedParams.queryParams = new HashMap<>();
    groupedParams.postFilterParams = new HashMap<>();

    searchRequest
        .getParameters()
        .forEach(
            (k, v) -> {
              if (searchRequest.getFacets().contains(k)) {
                groupedParams.postFilterParams.put(k, v);
              } else {
                groupedParams.queryParams.put(k, v);
              }
            });

    return groupedParams;
  }

  private Optional<QueryBuilder> buildPostFilter(
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams, boolean matchCase) {
    if (postFilterParams == null || postFilterParams.isEmpty()) {
      return Optional.empty();
    }

    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(
            postFilterParams.entrySet().stream()
                .flatMap(
                    e ->
                        buildTermQuery(
                            e.getValue(),
                            e.getKey(),
                            occurrenceBaseEsFieldMapper.getEsField(e.getKey()),
                            matchCase)
                            .stream())
                .collect(Collectors.toList()));

    return Optional.of(bool);
  }

  private Optional<List<AggregationBuilder>> buildAggs(
      OccurrenceSearchRequest searchRequest,
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams) {
    if (searchRequest.getFacets() == null || searchRequest.getFacets().isEmpty()) {
      return Optional.empty();
    }

    if (searchRequest.isMultiSelectFacets()
        && postFilterParams != null
        && !postFilterParams.isEmpty()) {
      return Optional.of(buildFacetsMultiselect(searchRequest, postFilterParams));
    }

    return Optional.of(buildFacets(searchRequest));
  }

  /** Creates a filter with all the filter in which the facet param is not present. */
  private BoolQueryBuilder getAggregationPostFilter(
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams,
      OccurrenceSearchParameter facetParam,
      boolean matchCase) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(
            postFilterParams.entrySet().stream()
                .filter(entry -> entry.getKey() != facetParam)
                .flatMap(
                    e ->
                        buildTermQuery(
                            e.getValue(),
                            e.getKey(),
                            occurrenceBaseEsFieldMapper.getEsField(e.getKey()),
                            matchCase)
                            .stream())
                .collect(Collectors.toList()));
    return bool;
  }

  /** Creates a filter with all the filter in which the facet param is not present. */
  private BoolQueryBuilder getAggregationFilter(
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams,
      OccurrenceSearchParameter facetParam,
      boolean matchCase,
      boolean wrappedChildrenQueries) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(
            buildTermQuery(
                Optional.ofNullable(postFilterParams.get(facetParam))
                    .orElse(Collections.emptySet()),
                facetParam,
                occurrenceBaseEsFieldMapper.getEsField(facetParam),
                matchCase,
                wrappedChildrenQueries));
    return bool;
  }

  private List<AggregationBuilder> buildFacetsMultiselect(
      OccurrenceSearchRequest searchRequest,
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams) {

    if (searchRequest.getFacets().size() == 1) {
      // same case as normal facets
      return buildFacets(searchRequest);
    }

    return searchRequest.getFacets().stream()
        .filter(p -> occurrenceBaseEsFieldMapper.getEsField(p) != null)
        .map(
            facetParam -> {

              // build filter aggs
              BoolQueryBuilder bool =
                  getAggregationPostFilter(
                      postFilterParams, facetParam, searchRequest.isMatchCase());

              // add filter to the aggs
              EsField esField = occurrenceBaseEsFieldMapper.getEsFacetField(facetParam);
              FilterAggregationBuilder filterAggs =
                  AggregationBuilders.filter(esField.getSearchFieldName(), bool);

              // build terms aggs and add it to the filter aggs
              TermsAggregationBuilder termsAggs =
                  buildTermsAggs(
                      "filtered_" + esField.getSearchFieldName(),
                      esField,
                      searchRequest,
                      facetParam);
              if (esField.isChildField()) {
                filterAggs.subAggregation(
                    getChildrenAggregationBuilder(
                        searchRequest, postFilterParams, facetParam, esField));
              } else {
                filterAggs.subAggregation(termsAggs);
              }

              return filterAggs;
            })
        .collect(Collectors.toList());
  }

  boolean isDynamicRankParam(OccurrenceSearchRequest searchRequest, OccurrenceSearchParameter p){

    if (searchRequest.getParameters().containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)){
      String checklistKey = searchRequest.getParameters().get(OccurrenceSearchParameter.CHECKLIST_KEY).iterator().next();
      for (String rank: triage.getChecklistRanks(checklistKey)){
        if (p.equals(new OccurrenceSearchParameter(rank.toUpperCase() + "_KEY", String.class))) {
          return true;
        }
      }
    }
    return false;
  }

  static boolean isTaxonDepthParam(OccurrenceSearchParameter p){
    return p.name().startsWith("TAXON_DEPTH_");
  }

  private List<AggregationBuilder> buildFacets(OccurrenceSearchRequest searchRequest) {
    final AtomicReference<GroupedParams> groupedParams = new AtomicReference<>();
    return searchRequest.getFacets().stream()
        .filter(p -> occurrenceBaseEsFieldMapper.getEsField(p) != null || isDynamicRankParam(searchRequest, p)
          || isTaxonDepthParam(p))
        .map(
            facetParam -> {

              EsField esField = occurrenceBaseEsFieldMapper.getEsFacetField(facetParam);
              if (esField != null && esField.isChildField()) {
                if (groupedParams.get() == null) {
                  groupedParams.set(groupParameters(searchRequest, true));
                }
                return getChildrenAggregationBuilder(
                    searchRequest, groupedParams.get().postFilterParams, facetParam, esField);
              }

              // if checklistKey is present, we need to substitute the field name
              if (searchRequest.getParameters().containsKey(OccurrenceSearchParameter.CHECKLIST_KEY) && isTaxonomic(facetParam)) {

                String checklistKey = searchRequest.getParameters().get(OccurrenceSearchParameter.CHECKLIST_KEY).iterator().next();
                String esFieldToUse = null;

                if (esField.getTerm().equals(GbifTerm.taxonKey)){
                  esFieldToUse = "classifications." + checklistKey + ".taxonKeys.keyword";
                } else if (rankToField.containsKey(esField.getTerm())){
                  esFieldToUse = "classifications."
                    + checklistKey + ".classificationKeys." + rankToField.get(esField.getTerm()).toUpperCase() + ".keyword";
                } else {
                  return buildTermsAggs(
                    esField.getSearchFieldName(), esField, searchRequest, facetParam);
                }

                return buildTermsAggs(
                    esField.getSearchFieldName(), esFieldToUse, searchRequest, facetParam);
              } else if (searchRequest.getParameters().containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)
                && isDynamicRankParam(searchRequest, facetParam)) {
                  String checklistKey = searchRequest.getParameters().get(OccurrenceSearchParameter.CHECKLIST_KEY).iterator().next();
                  String rank = facetParam.name().replace("_KEY", "");
                  String esFieldToUse = "classifications." + checklistKey + ".classificationKeys." + rank.toUpperCase() + ".keyword";
                  return buildTermsAggs(
                    facetParam.name(),
                    esFieldToUse,
                    searchRequest,
                    facetParam);
              } else if (searchRequest.getParameters().containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)
                && isTaxonDepthParam(facetParam)) {

                String checklistKey = searchRequest.getParameters().get(OccurrenceSearchParameter.CHECKLIST_KEY).iterator().next();
                String depth = facetParam.name().replace("TAXON_DEPTH_", "");
                String esFieldToUse = "classifications." + checklistKey + ".classificationDepth." + depth + ".keyword";
                return buildTermsAggs(
                  facetParam.name(),
                  esFieldToUse,
                  searchRequest,
                  facetParam);

              } else {
                return buildTermsAggs(
                  esField.getSearchFieldName(), esField, searchRequest, facetParam);
              }
            })
        .collect(Collectors.toList());
  }

  static Map<GbifTerm, String> rankToField = Map.of(
    GbifTerm.speciesKey, "species",
    GbifTerm.subgenusKey, "subgenus",
    GbifTerm.genusKey, "genus",
    GbifTerm.familyKey, "family",
    GbifTerm.orderKey, "order",
    GbifTerm.classKey, "class",
    GbifTerm.phylumKey, "phylum",
    GbifTerm.kingdomKey, "kingdom"
  );


  private boolean isTaxonomic(OccurrenceSearchParameter param) {
    return param == OccurrenceSearchParameter.ACCEPTED_TAXON_KEY
        || param == OccurrenceSearchParameter.TAXON_KEY;
  }

  private ChildrenAggregationBuilder getChildrenAggregationBuilder(
      OccurrenceSearchRequest searchRequest,
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams,
      OccurrenceSearchParameter facetParam,
      EsField esField) {
    if (postFilterParams.containsKey(facetParam)) {
      return new ChildrenAggregationBuilder(
              esField.getSearchFieldName(), esField.childrenRelation())
          .subAggregation(
              AggregationBuilders.filter(
                      esField.getSearchFieldName(),
                      getAggregationFilter(
                          postFilterParams, facetParam, searchRequest.isMatchCase(), false))
                  .subAggregation(
                      buildTermsAggs(
                          esField.getSearchFieldName(), esField, searchRequest, facetParam)));
    }
    return new ChildrenAggregationBuilder(esField.getSearchFieldName(), esField.childrenRelation())
        .subAggregation(
            buildTermsAggs(esField.getSearchFieldName(), esField, searchRequest, facetParam));
  }

  private TermsAggregationBuilder buildTermsAggs(
      String aggName,
      EsField esField,
      OccurrenceSearchRequest searchRequest,
      OccurrenceSearchParameter facetParam) {
    String fieldName = searchRequest.isMatchCase() ? esField.getVerbatimFieldName() : esField.getExactMatchFieldName();
    return buildTermsAggs(aggName, fieldName, searchRequest, facetParam);
  }

  private static TermsAggregationBuilder buildTermsAggs(String aggName, String fieldName, OccurrenceSearchRequest searchRequest, OccurrenceSearchParameter facetParam ) {
    // build aggs for the field
    TermsAggregationBuilder termsAggsBuilder =
        AggregationBuilders.terms(aggName)
            .field(fieldName);

    // min count
    Optional.ofNullable(searchRequest.getFacetMinCount()).ifPresent(termsAggsBuilder::minDocCount);

    // aggs size
    int size =
        calculateAggsSize(
            facetParam,
            extractFacetOffset(searchRequest, facetParam),
            extractFacetLimit(searchRequest, facetParam));
    termsAggsBuilder.size(size);

    // aggs shard size
    termsAggsBuilder.shardSize(
        CARDINALITIES.getOrDefault(facetParam, DEFAULT_SHARD_SIZE.applyAsInt(size)));

    return termsAggsBuilder;
  }

  private static int calculateAggsSize(
      OccurrenceSearchParameter facetParam, int facetOffset, int facetLimit) {
    int maxCardinality = CARDINALITIES.getOrDefault(facetParam, Integer.MAX_VALUE);

    // the limit is bounded by the max cardinality of the field
    int limit = Math.min(facetOffset + facetLimit, maxCardinality);

    // we set a maximum limit for performance reasons
    if (limit > MAX_SIZE_TERMS_AGGS) {
      throw new IllegalArgumentException(
          "Facets paging is only supported up to " + MAX_SIZE_TERMS_AGGS + " elements");
    }
    return limit;
  }

  /**
   * Mapping parameter values into know values for Enums. Non-enum parameter values are passed using
   * its raw value.
   */
  private String parseParamValue(String value, OccurrenceSearchParameter parameter) {
    if (Enum.class.isAssignableFrom(parameter.type())
        && !Country.class.isAssignableFrom(parameter.type())) {
      return VocabularyUtils.lookup(value, (Class<Enum<?>>) parameter.type())
          .map(Enum::name)
          .orElse(null);
    }
    if (Boolean.class.isAssignableFrom(parameter.type())) {
      return value.toLowerCase();
    }
    return value;
  }

  private List<QueryBuilder> buildTermQuery(
      Collection<String> values,
      OccurrenceSearchParameter param,
      EsField esField,
      boolean matchCase) {
    return buildTermQuery(values, param, esField, matchCase, true);
  }

  private List<QueryBuilder> buildTermQuery(
      Collection<String> values,
      OccurrenceSearchParameter param,
      EsField esField,
      boolean matchCase,
      boolean wrappedChildrenQueries) {
    List<QueryBuilder> queries = new ArrayList<>();

    // collect queries for each value
    List<String> parsedValues = new ArrayList<>();
    for (String value : values) {
      if (isNumericRange(value)) {
        RangeQueryBuilder rangeQueryBuilder = buildRangeQuery(esField, value);
        if (occurrenceBaseEsFieldMapper.includeNullInRange(param, rangeQueryBuilder)) {
          queries.add(
            QueryBuilders.boolQuery()
              .should(buildRangeQuery(esField, value))
              .should(
                QueryBuilders.boolQuery()
                  .mustNot(QueryBuilders.existsQuery(esField.getExactMatchFieldName()))));
        } else {
          queries.add(buildRangeQuery(esField, value));
        }
        continue;
      } else if (occurrenceBaseEsFieldMapper.isDateField(esField)) {
        RangeQueryBuilder rangeQueryBuilder = buildRangeQuery(esField, value);
        if (occurrenceBaseEsFieldMapper.includeNullInRange(param, rangeQueryBuilder)) {
          queries.add(
              QueryBuilders.boolQuery()
                  .should(buildRangeQuery(esField, value))
                  .should(
                      QueryBuilders.boolQuery()
                          .mustNot(QueryBuilders.existsQuery(esField.getExactMatchFieldName()))));
        } else {
          queries.add(buildRangeQuery(esField, value));
        }
        continue;
      }

      parsedValues.add(parseParamValue(value, param));
    }

    String fieldName =
        matchCase ? esField.getVerbatimFieldName() : esField.getExactMatchFieldName();
    if (parsedValues.size() == 1) {
      // single term
      queries.add(QueryBuilders.termQuery(fieldName, parsedValues.get(0)));
    } else if (parsedValues.size() > 1) {
      // multi term query
      queries.add(QueryBuilders.termsQuery(fieldName, parsedValues));
    }

    if (wrappedChildrenQueries && esField.isChildField()) {
      return queries.stream()
          .map(q -> JoinQueryBuilders.hasChildQuery(esField.childrenRelation(), q, ScoreMode.None))
          .collect(Collectors.toList());
    }
    return queries;
  }

  private RangeQueryBuilder buildRangeQuery(EsField esField, String value) {
    RangeQueryBuilder builder = QueryBuilders.rangeQuery(esField.getExactMatchFieldName());

    if (occurrenceBaseEsFieldMapper.isDateField(esField)) {
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(value);

      if (dateRange.hasLowerBound()) {
        builder.gte(dateRange.lowerEndpoint());
      }

      if (dateRange.hasUpperBound()) {
        builder.lt(dateRange.upperEndpoint());
      }
      // For a match, the occurrence's date range must be entirely within the search query date
      // range.
      // i.e. Q:eventDate=1980 will match rec:eventDate=1980-02, but not
      // rec:eventDate=1980-10-01/1982-02-02.
      builder.relation("within");
    } else {
      String[] values = value.split(RANGE_SEPARATOR);
      if (!RANGE_WILDCARD.equals(values[0])) {
        builder.gte(values[0]);
      }
      if (!RANGE_WILDCARD.equals(values[1])) {
        builder.lte(values[1]);
      }
    }

    return builder;
  }

  public GeoDistanceQueryBuilder buildGeoDistanceQuery(String rawGeoDistance) {
    DistanceUnit.GeoDistance geoDistance =
        DistanceUnit.GeoDistance.parseGeoDistance(rawGeoDistance);
    return buildGeoDistanceQuery(geoDistance);
  }

  public GeoDistanceQueryBuilder buildGeoDistanceQuery(DistanceUnit.GeoDistance geoDistance) {
    return QueryBuilders.geoDistanceQuery(
            occurrenceBaseEsFieldMapper.getGeoDistanceEsField().getSearchFieldName())
        .distance(geoDistance.getDistance().toString())
        .point(geoDistance.getLatitude(), geoDistance.getLongitude());
  }

  public GeoShapeQueryBuilder buildGeoShapeQuery(String wkt) {
    Geometry geometry;
    try {
      geometry = new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    Function<Polygon, PolygonBuilder> polygonToBuilder =
        polygon -> {
          PolygonBuilder polygonBuilder =
              new PolygonBuilder(
                  new CoordinatesBuilder()
                      .coordinates(
                          normalizePolygonCoordinates(polygon.getExteriorRing().getCoordinates())));
          for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            polygonBuilder.hole(
                new LineStringBuilder(
                    new CoordinatesBuilder()
                        .coordinates(
                            normalizePolygonCoordinates(
                                polygon.getInteriorRingN(i).getCoordinates()))));
          }
          return polygonBuilder;
        };

    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LINESTRING"
            : geometry.getGeometryType().toUpperCase();

    ShapeBuilder shapeBuilder = null;
    if (("POINT").equals(type)) {
      shapeBuilder = new PointBuilder(geometry.getCoordinate().x, geometry.getCoordinate().y);
    } else if ("LINESTRING".equals(type)) {
      shapeBuilder = new LineStringBuilder(Arrays.asList(geometry.getCoordinates()));
    } else if ("POLYGON".equals(type)) {
      shapeBuilder = polygonToBuilder.apply((Polygon) geometry);
    } else if ("MULTIPOLYGON".equals(type)) {
      // multipolygon
      MultiPolygonBuilder multiPolygonBuilder = new MultiPolygonBuilder();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        multiPolygonBuilder.polygon(polygonToBuilder.apply((Polygon) geometry.getGeometryN(i)));
      }
      shapeBuilder = multiPolygonBuilder;
    } else {
      throw new IllegalArgumentException(type + " shape is not supported");
    }

    try {
      return QueryBuilders.geoShapeQuery(
              occurrenceBaseEsFieldMapper.getGeoShapeEsField().getSearchFieldName(),
              shapeBuilder.buildGeometry())
          .relation(ShapeRelation.WITHIN);
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  /** Eliminates consecutive duplicates. The order is preserved. */
  @VisibleForTesting
  static Coordinate[] normalizePolygonCoordinates(Coordinate[] coordinates) {
    List<Coordinate> normalizedCoordinates = new ArrayList<>();

    // we always have to keep the fist and last coordinates
    int i = 0;
    normalizedCoordinates.add(i++, coordinates[0]);

    for (int j = 1; j < coordinates.length; j++) {
      if (!coordinates[j - 1].equals(coordinates[j])) {
        normalizedCoordinates.add(i++, coordinates[j]);
      }
    }

    return normalizedCoordinates.toArray(new Coordinate[0]);
  }

  @VisibleForTesting
  static class GroupedParams {
    Map<OccurrenceSearchParameter, Set<String>> postFilterParams;
    Map<OccurrenceSearchParameter, Set<String>> queryParams;
  }
}
