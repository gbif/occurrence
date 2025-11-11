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
import static org.gbif.occurrence.search.es.EsQueryUtils.*;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.*;
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
import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.PredicateSearchRequest;
import org.gbif.api.model.common.search.SearchConstants;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.Range;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.predicate.query.EsField;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.EsQueryVisitor;
import org.gbif.rest.client.species.Metadata;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.BaseEsField;
import org.gbif.search.es.ChecklistEsField;
import org.gbif.search.es.event.EventEsField;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.vocabulary.client.ConceptClient;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public abstract class BaseEsSearchRequestBuilder<
    P extends SearchParameter, S extends FacetedSearchRequest<P>> {

  private static final int MAX_SIZE_TERMS_AGGS = 1200000;
  private static final IntUnaryOperator DEFAULT_SHARD_SIZE = size -> (size * 2) + 50000;

  public static String[] SOURCE_EXCLUDE =
      new String[] {"all", "notIssues", "*.verbatim", "*.suggest"};

  protected final EsFieldMapper<P> esFieldMapper;
  protected final NameUsageMatchingService nameUsageMatchingService;
  protected final ConceptClient conceptClient;
  protected final EsQueryVisitor<P> esQueryVisitor;

  public BaseEsSearchRequestBuilder(
      EsFieldMapper<P> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      EsQueryVisitor<P> esQueryVisitor) {
    this.esFieldMapper = esFieldMapper;
    this.conceptClient = conceptClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
    this.esQueryVisitor = esQueryVisitor;
  }

  public SearchRequest buildSearchRequest(S searchRequest, String index) {

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
    GroupedParams<P> groupedParams = groupParameters(searchRequest);

    // checklistKey to be used later
    String checklistKey = getChecklistKey(searchRequest.getParameters());

    // add query
    if (searchRequest instanceof PredicateSearchRequest) {
      buildQuery((PredicateSearchRequest) searchRequest).ifPresent(searchSourceBuilder::query);
    } else {
      buildQuery(
              groupedParams.queryParams,
              searchRequest.getQ(),
              searchRequest.isMatchCase(),
              checklistKey)
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
      esFieldMapper.getDefaultSort().forEach(searchSourceBuilder::sort);
    } else {
      searchSourceBuilder.sort(SortBuilders.scoreSort());
    }

    // add aggs
    buildAggs(searchRequest, groupedParams.postFilterParams)
        .ifPresent(aggsList -> aggsList.forEach(searchSourceBuilder::aggregation));

    // post-filter
    buildPostFilter(groupedParams.postFilterParams, searchRequest.isMatchCase(), checklistKey)
        .ifPresent(searchSourceBuilder::postFilter);

    return esRequest;
  }

  public Optional<QueryBuilder> buildQueryNode(S searchRequest) {
    return buildQuery(
        searchRequest.getParameters(),
        searchRequest.getQ(),
        searchRequest.isMatchCase(),
        getChecklistKey(searchRequest.getParameters()));
  }

  SearchRequest buildSuggestQuery(String prefix, P parameter, Integer limit, String index) {
    SearchRequest request = new SearchRequest();
    request.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    request.source(searchSourceBuilder);

    EsField esField = esFieldMapper.getEsField(parameter);

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
  private void addChecklistKeyParamToQuery(
      Map<P, Set<String>> params, BoolQueryBuilder bool, P taxonParam) {

    if (params.containsKey(taxonParam)
        && params.get(taxonParam) != null
        && !params.get(taxonParam).isEmpty()) {
      String checklistKey = getChecklistKey(params);
      String esFieldToUse = esFieldMapper.getChecklistField(checklistKey, taxonParam);
      // Build the query
      BoolQueryBuilder checklistQuery =
          QueryBuilders.boolQuery()
              .must(QueryBuilders.termsQuery(esFieldToUse, params.get(taxonParam)));

      EsField esField = esFieldMapper.getEsField(taxonParam);
      // TODO: this has to be with the other nested queries
      if (esField.isNestedField()) {
        bool.filter()
            .add(
                QueryBuilders.nestedQuery(esField.getNestedPath(), checklistQuery, ScoreMode.None));
      } else {
        bool.filter().add(checklistQuery);
      }
    }
  }

  /**
   * Add a taxon key query to the builder
   *
   * @param rank
   * @param bool
   * @param params
   */
  private void addTaxonKeyQuery(String rank, BoolQueryBuilder bool, Map<P, Set<String>> params) {

    String checklistKey = getChecklistKey(params);

    P dynamicRankParam = createSearchParam(rank.toUpperCase() + "_KEY", String.class);
    if (params.containsKey(dynamicRankParam)) {
      BoolQueryBuilder checklistQuery =
          QueryBuilders.boolQuery()
              .must(
                  QueryBuilders.termsQuery(
                      ((ChecklistEsField) OccurrenceEsField.TAXON_KEY.getEsField())
                          .getSearchFieldName(checklistKey),
                      params.get(dynamicRankParam)));
      params.remove(dynamicRankParam);
      bool.filter().add(checklistQuery);
    }
  }

  @VisibleForTesting
  Optional<QueryBuilder> buildQuery(
      Map<P, Set<String>> params, String qParam, boolean matchCase, String checklistKey) {
    translateFields(params);

    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(esFieldMapper.getFullTextField(), qParam));
    }

    if (params != null && !params.isEmpty()) {

      // check for dynamic ranks e.g. subphylum that will be specific to a checklist
      addChecklistDynamicRanks(params, bool);

      // handle the switch from issue -> non-taxonomic issue if checklistKey supplied
      handleIssueQueries(params, bool);

      // adding geometry to bool
      getParam("GEOMETRY")
          .filter(params::containsKey)
          .ifPresent(
              param -> {
                BoolQueryBuilder shouldGeometry = QueryBuilders.boolQuery();
                shouldGeometry
                    .should()
                    .addAll(params.get(param).stream().map(this::buildGeoShapeQuery).toList());
                bool.filter().add(shouldGeometry);
              });

      getParam("GEO_DISTANCE")
          .filter(params::containsKey)
          .ifPresent(
              param -> {
                BoolQueryBuilder shouldGeoDistance = QueryBuilders.boolQuery();
                shouldGeoDistance
                    .should()
                    .addAll(params.get(param).stream().map(this::buildGeoDistanceQuery).toList());
                bool.filter().add(shouldGeoDistance);
              });

      // adding term queries to bool
      bool.filter().addAll(createQueries(params, matchCase, checklistKey));
    }
    esFieldMapper.getDefaultFilter().ifPresent(df -> bool.filter().add(df));

    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  private List<QueryBuilder> createQueries(
      Map<P, Set<String>> params, boolean matchCase, String checklistKey) {
    return createQueries(params, matchCase, true, checklistKey);
  }

  private List<QueryBuilder> createQueries(
      Map<P, Set<String>> params,
      boolean matchCase,
      boolean wrappedChildrenQueries,
      String checklistKey) {
    Map<String, List<QueryBuilder>> queriesByNestedPath = new HashMap<>();
    List<QueryBuilder> nonNestedQueries = new ArrayList<>();
    for (Map.Entry<P, Set<String>> e : params.entrySet()) {
      EsField esField = esFieldMapper.getEsField(e.getKey());
      if (esField == null) {
        continue;
      }
      List<QueryBuilder> queryBuilders =
          buildTermQuery(
              e.getValue(),
              e.getKey(),
              esFieldMapper.getEsField(e.getKey()),
              matchCase,
              wrappedChildrenQueries,
              checklistKey);

      if (esField.isNestedField()) {
        queriesByNestedPath
            .computeIfAbsent(esField.getNestedPath(), k -> new ArrayList<>())
            .addAll(queryBuilders);
      } else {
        nonNestedQueries.addAll(queryBuilders);
      }
    }

    List<QueryBuilder> allQueries = new ArrayList<>(nonNestedQueries);

    // add nested queries
    queriesByNestedPath.forEach(
        (key, value) -> {
          BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();
          value.forEach(q -> nestedBoolQuery.filter().add(q));
          allQueries.add(QueryBuilders.nestedQuery(key, nestedBoolQuery, ScoreMode.None));
        });

    return allQueries;
  }

  protected abstract void handleIssueQueries(Map<P, Set<String>> params, BoolQueryBuilder bool);

  /**
   * Retrieve the checklistKey from the request or fallback to the configured default.
   *
   * @param params
   * @return
   */
  public String getChecklistKey(Map<P, Set<String>> params) {
    return getParam("CHECKLIST_KEY")
        .filter(params::containsKey)
        .map(p -> params.get(p).iterator().next())
        .orElse(esFieldMapper.getDefaultChecklistKey());
  }

  /**
   * Checks the request parameters for any dynamic ranks and adds them to the query e.g.
   * SUBPHYLUM_KEY=XXXXX
   *
   * @param params
   * @param bool
   */
  private void addChecklistDynamicRanks(Map<P, Set<String>> params, BoolQueryBuilder bool) {
    if (nameUsageMatchingService != null) {
      Optional.ofNullable(nameUsageMatchingService.getMetadata(getChecklistKey(params)))
          .ifPresent(
              metadata ->
                  metadata
                      .getMainIndex()
                      .getNameUsageByRankCount()
                      .keySet()
                      .forEach(
                          rank -> {
                            addTaxonKeyQuery(rank, bool, params);
                          }));
    }
  }

  @SneakyThrows
  public Optional<BoolQueryBuilder> buildQuery(PredicateSearchRequest searchRequest) {
    searchRequest.setPredicate(translatePredicateFields(searchRequest.getPredicate()));

    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    String qParam = searchRequest.getQ();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(esFieldMapper.getFullTextField(), qParam));
    }

    esQueryVisitor.getQueryBuilder(searchRequest.getPredicate()).ifPresent(bool::must);

    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  @VisibleForTesting
  GroupedParams<P> groupParameters(S searchRequest) {
    return groupParameters(searchRequest, searchRequest.isFacetMultiSelect());
  }

  GroupedParams<P> groupParameters(S searchRequest, boolean groupFilters) {
    GroupedParams<P> groupedParams = new GroupedParams<>();

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
      Map<P, Set<String>> postFilterParams, boolean matchCase, String checklistKey) {
    if (postFilterParams == null || postFilterParams.isEmpty()) {
      return Optional.empty();
    }

    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter().addAll(createQueries(postFilterParams, matchCase, checklistKey));

    return Optional.of(bool);
  }

  private Optional<List<AggregationBuilder>> buildAggs(
      S searchRequest, Map<P, Set<String>> postFilterParams) {
    if (searchRequest.getFacets() == null || searchRequest.getFacets().isEmpty()) {
      return Optional.empty();
    }

    if (searchRequest.isFacetMultiSelect()
        && postFilterParams != null
        && !postFilterParams.isEmpty()) {
      return Optional.of(buildFacetsMultiselect(searchRequest, postFilterParams));
    }

    return Optional.of(buildFacets(searchRequest));
  }

  /** Creates a filter with all the filter in which the facet param is not present. */
  private BoolQueryBuilder getAggregationPostFilter(
      Map<P, Set<String>> postFilterParams, P facetParam, boolean matchCase, String checklistKey) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();

    Map<P, Set<String>> filteredParams =
        postFilterParams.entrySet().stream()
            .filter(entry -> entry.getKey() != facetParam)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    bool.filter().addAll(createQueries(filteredParams, matchCase, checklistKey));
    return bool;
  }

  /** Creates a filter with all the filter in which the facet param is not present. */
  private BoolQueryBuilder getAggregationFilter(
      Map<P, Set<String>> postFilterParams,
      P facetParam,
      boolean matchCase,
      boolean wrappedChildrenQueries,
      String checklistKey) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(
            createQueries(
                Map.of(
                    facetParam,
                    Optional.ofNullable(postFilterParams.get(facetParam))
                        .orElse(Collections.emptySet())),
                matchCase,
                wrappedChildrenQueries,
                checklistKey));
    return bool;
  }

  private List<AggregationBuilder> buildFacetsMultiselect(
      S searchRequest, Map<P, Set<String>> postFilterParams) {

    if (searchRequest.getFacets().size() == 1) {
      // same case as normal facets
      return buildFacets(searchRequest);
    }

    return searchRequest.getFacets().stream()
        .filter(p -> esFieldMapper.getEsField(p) != null)
        .map(
            facetParam -> {

              // build filter aggs
              BoolQueryBuilder bool =
                  getAggregationPostFilter(
                      postFilterParams,
                      facetParam,
                      searchRequest.isMatchCase(),
                      getChecklistKey(searchRequest.getParameters()));

              // add filter to the aggs
              EsField esField = esFieldMapper.getEsFacetField(facetParam);
              FilterAggregationBuilder filterAggs =
                  AggregationBuilders.filter(esField.getSearchFieldName(), bool);

              // build terms aggs and add it to the filter aggs
              AggregationBuilder termsAggs =
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

  boolean isDynamicRankParam(S searchRequest, P p) {
    // TODO: Use the one from the enum so it doesn't compile if changes
    return getParam("CHECKLIST_KEY")
        .filter(param -> searchRequest.getParameters().containsKey(param))
        .map(
            param -> {
              String checklistKey = searchRequest.getParameters().get(param).iterator().next();
              if (nameUsageMatchingService != null) {
                // get the metadata for the checklistKey
                Metadata metadata = nameUsageMatchingService.getMetadata(checklistKey);
                if (metadata != null) {
                  Set<String> ranks = metadata.getMainIndex().getNameUsageByRankCount().keySet();
                  for (String rank : ranks) {
                    if (p.equals(createSearchParam(rank.toUpperCase() + "_KEY", String.class))) {
                      return true;
                    }
                  }
                }
              }
              return false;
            })
        .orElse(false);
  }

  private List<AggregationBuilder> buildFacets(S searchRequest) {
    final AtomicReference<GroupedParams<P>> groupedParams = new AtomicReference<>();
    return searchRequest.getFacets().stream()
        .filter(p -> esFieldMapper.getEsField(p) != null || isDynamicRankParam(searchRequest, p))
        .map(
            facetParam -> {
              EsField esField = esFieldMapper.getEsFacetField(facetParam);
              if (esField != null && esField.isChildField()) {
                if (groupedParams.get() == null) {
                  groupedParams.set(groupParameters(searchRequest, true));
                }
                return getChildrenAggregationBuilder(
                    searchRequest, groupedParams.get().postFilterParams, facetParam, esField);
              }

              // if a checklist has been supplied, then use the non taxonomic issues field
              if (getParam("ISSUE").map(facetParam::equals).orElse(false)
                  && getParam("CHECKLIST_KEY")
                      .map(p -> searchRequest.getParameters().containsKey(p))
                      .orElse(false)) {
                return buildTermsAggs(
                    OccurrenceEsField.NON_TAXONOMIC_ISSUE.getSearchFieldName(),
                    OccurrenceEsField.NON_TAXONOMIC_ISSUE,
                    searchRequest,
                    facetParam);
              }

              // handle taxonomy based fields
              if (isTaxonomic(facetParam)) {
                BaseEsField field = null;
                String esFieldName = null;
                if (esField instanceof OccurrenceEsField) {
                  field = ((OccurrenceEsField) esField).getEsField();
                  esFieldName = ((OccurrenceEsField) esField).name();
                } else if (esField instanceof EventEsField) {
                  field = ((EventEsField) esField).getEsField();
                  esFieldName = ((EventEsField) esField).name();
                }

                if (field instanceof ChecklistEsField) {
                  return buildTermsAggs(esFieldName, field, searchRequest, facetParam);
                } else {
                  throw new IllegalArgumentException(
                      "Facet "
                          + facetParam
                          + " is not a valid taxonomy field. Use the checklist field instead.");
                }
              }

              if (isDynamicRankParam(searchRequest, facetParam)) {
                String esFieldToUse =
                    String.format(
                        "classifications.%s.classificationKeys.%s",
                        getChecklistKey(searchRequest.getParameters()),
                        facetParam
                            .name()
                            .substring(0, facetParam.name().length() - 4)
                            .toUpperCase());

                return buildTermsAggs(facetParam.name(), esFieldToUse, searchRequest, facetParam);
              }

              return buildTermsAggs(
                  esField.getSearchFieldName(), esField, searchRequest, facetParam);
            })
        .collect(Collectors.toList());
  }

  private boolean isTaxonomic(P param) {
    return esFieldMapper.isTaxonomic(param);
  }

  private ChildrenAggregationBuilder getChildrenAggregationBuilder(
      S searchRequest, Map<P, Set<String>> postFilterParams, P facetParam, EsField esField) {
    if (postFilterParams.containsKey(facetParam)) {
      return new ChildrenAggregationBuilder(
              esField.getSearchFieldName(), esField.childrenRelation())
          .subAggregation(
              AggregationBuilders.filter(
                      esField.getSearchFieldName(),
                      getAggregationFilter(
                          postFilterParams,
                          facetParam,
                          searchRequest.isMatchCase(),
                          false,
                          getChecklistKey(searchRequest.getParameters())))
                  .subAggregation(
                      buildTermsAggs(
                          esField.getSearchFieldName(), esField, searchRequest, facetParam)));
    }
    return new ChildrenAggregationBuilder(esField.getSearchFieldName(), esField.childrenRelation())
        .subAggregation(
            buildTermsAggs(esField.getSearchFieldName(), esField, searchRequest, facetParam));
  }

  private AggregationBuilder buildTermsAggs(
      String aggName, EsField esField, S searchRequest, P facetParam) {

    String fieldName = null;
    if (esField instanceof ChecklistEsField) {
      fieldName =
          ((ChecklistEsField) esField)
              .getSearchFieldName(getChecklistKey(searchRequest.getParameters()));
    } else {
      fieldName =
          searchRequest.isMatchCase()
              ? esField.getVerbatimFieldName()
              : esField.getExactMatchFieldName();
    }

    return buildTermsAggs(aggName, fieldName, searchRequest, facetParam, esField);
  }

  private AggregationBuilder buildTermsAggs(
      String aggName, String fieldName, S searchRequest, P facetParam) {
    return buildTermsAggs(aggName, fieldName, searchRequest, facetParam, null);
  }

  private AggregationBuilder buildTermsAggs(
      String aggName, String fieldName, S searchRequest, P facetParam, EsField esField) {
    // build aggs for the field
    TermsAggregationBuilder termsAggsBuilder = AggregationBuilders.terms(aggName).field(fieldName);

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

    if (esField != null && esField.isNestedField()) {
      return AggregationBuilders.nested(facetParam.name(), esField.getNestedPath())
          .subAggregation(termsAggsBuilder);
    } else {
      return termsAggsBuilder;
    }
  }

  private int calculateAggsSize(P facetParam, int facetOffset, int facetLimit) {
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
  private String parseParamValue(String value, P parameter) {
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
      P param,
      EsField esField,
      boolean matchCase,
      boolean wrappedChildrenQueries,
      String checklistKey) {
    List<QueryBuilder> queries = new ArrayList<>();

    // collect queries for each value
    List<String> parsedValues = new ArrayList<>();

    List<QueryBuilder> queryBuilders = new ArrayList<>();
    for (String value : values) {
      if (isNumericRange(value) || esFieldMapper.isDateField(esField)) {
        RangeQueryBuilder rangeQueryBuilder = buildRangeQuery(esField, value);
        queryBuilders.add(rangeQueryBuilder);
        if (esFieldMapper.includeNullInRange(param, rangeQueryBuilder)) {
          queryBuilders.add(
              QueryBuilders.boolQuery()
                  .mustNot(QueryBuilders.existsQuery(esField.getExactMatchFieldName())));
        }
        continue;
      }
      parsedValues.add(parseParamValue(value, param));
    }

    if (!queryBuilders.isEmpty()) {
      BoolQueryBuilder ranges = QueryBuilders.boolQuery();
      queryBuilders.forEach(ranges::should);
      queries.add(ranges);
    }

    // get the field name based on the taxonomy and the matchCase
    String fieldName = null;
    if (isTaxonomic(param)) {
      fieldName = esFieldMapper.getChecklistField(checklistKey, param);
    } else {
      fieldName = matchCase ? esField.getVerbatimFieldName() : esField.getExactMatchFieldName();
    }

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

    if (esFieldMapper.isDateField(esField)) {
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
      builder.relation(EsQueryUtils.WITHIN);
    } else {
      String[] values = value.split(RANGE_SEPARATOR);
      if (!RANGE_WILDCARD.equals(values[0])) {
        builder.gte(values[0]);
      }
      if (!RANGE_WILDCARD.equals(values[1])) {
        builder.lte(values[1]);
      }

      if (esField
          .getSearchFieldName()
          .equals(OccurrenceEsField.GEOLOGICAL_TIME.getSearchFieldName())) {
        builder.relation(EsQueryUtils.WITHIN);
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
            esFieldMapper.getGeoDistanceEsField().getSearchFieldName())
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
              esFieldMapper.getGeoShapeEsField().getSearchFieldName(), shapeBuilder.buildGeometry())
          .relation(ShapeRelation.WITHIN);
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  protected abstract Optional<P> getParam(String name);

  protected abstract P createSearchParam(String name, Class<?> type);

  protected abstract void translateFields(Map<P, Set<String>> params);
  protected abstract Predicate translatePredicateFields(Predicate predicate);

  protected void handleOccurrenceIssueQueries(Map<OccurrenceSearchParameter, Set<String>> params, BoolQueryBuilder bool) {
    if (params.containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)
      && params.containsKey(OccurrenceSearchParameter.ISSUE)) {
      String esFieldToUse = OccurrenceEsField.NON_TAXONOMIC_ISSUE.getSearchFieldName();

      // validate the value  - make sure it isn't taxonomic, otherwise throw an error
      params.get(OccurrenceSearchParameter.ISSUE).forEach(issue -> {
        OccurrenceIssue occurrenceIssue = OccurrenceIssue.valueOf(issue);
        if (OccurrenceIssue.TAXONOMIC_RULES.contains(occurrenceIssue)) {
          throw new IllegalArgumentException(
            "Please use TAXONOMIC_ISSUE parameter instead of ISSUE parameter " +
              " when using a checklistKey");
        }
      });

      // Build the query
      BoolQueryBuilder checklistQuery = QueryBuilders.boolQuery()
        .must(QueryBuilders.termsQuery(esFieldToUse, params.get(OccurrenceSearchParameter.ISSUE))
        );
      bool.filter().add(checklistQuery);
      params.remove(OccurrenceSearchParameter.ISSUE);
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
  static class GroupedParams<T extends SearchParameter> {
    Map<T, Set<String>> postFilterParams;
    Map<T, Set<String>> queryParams;
  }
}
