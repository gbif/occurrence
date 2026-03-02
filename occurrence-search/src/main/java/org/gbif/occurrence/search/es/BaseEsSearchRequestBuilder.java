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

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch._types.ScriptLanguage;
import co.elastic.clients.elasticsearch._types.ScriptSortType;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeRelation;
import co.elastic.clients.json.JsonData;
import com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
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
  protected final String defaultChecklistKey;

  public BaseEsSearchRequestBuilder(
      EsFieldMapper<P> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      EsQueryVisitor<P> esQueryVisitor,
      String defaultChecklistKey) {
    this.esFieldMapper = esFieldMapper;
    this.conceptClient = conceptClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
    this.esQueryVisitor = esQueryVisitor;
    this.defaultChecklistKey = defaultChecklistKey;
  }

  public SearchRequest buildSearchRequest(S searchRequest, String index) {
    SearchRequest.Builder searchRequestBuilder =
        new SearchRequest.Builder()
            .index(index)
            .size(searchRequest.getLimit())
            .from((int) searchRequest.getOffset())
            .trackTotalHits(t -> t.enabled(true))
            .source(src -> src.filter(f -> f.excludes(List.of(SOURCE_EXCLUDE))));

    // group params
    GroupedParams<P> groupedParams = groupParameters(searchRequest);

    // checklistKey to be used later
    String checklistKey = getChecklistKey(searchRequest.getParameters());

    // add query
    if (searchRequest instanceof PredicateSearchRequest) {
      buildQuery((PredicateSearchRequest) searchRequest).ifPresent(searchRequestBuilder::query);
    } else {
      buildQuery(
              groupedParams.queryParams,
              searchRequest.getQ(),
              searchRequest.isMatchCase(),
              checklistKey)
          .ifPresent(searchRequestBuilder::query);
    }

    // sort
    if (!Strings.isNullOrEmpty(searchRequest.getShuffle())) {
      // random score
      searchRequestBuilder.sort(
          s ->
              s.script(
                  ss ->
                      ss.type(ScriptSortType.Number)
                          .script(
                              sc ->
                                  sc.lang(ScriptLanguage.Painless)
                                      .source(
                                          src ->
                                              src.scriptString(
                                                  "(doc['_id'].value + params['seed']).hashCode()"))
                                      .params("seed", JsonData.of(searchRequest.getShuffle())))));
    } else if (Strings.isNullOrEmpty(searchRequest.getQ())) {
      esFieldMapper.getDefaultSort().forEach(searchRequestBuilder::sort);
    } else {
      searchRequestBuilder.sort(s -> s.score(sc -> sc));
    }

    // add aggs
    buildAggs(searchRequest, groupedParams.postFilterParams)
        .ifPresent(searchRequestBuilder::aggregations);

    // post-filter
    buildPostFilter(groupedParams.postFilterParams, searchRequest.isMatchCase(), checklistKey)
        .ifPresent(searchRequestBuilder::postFilter);

    return searchRequestBuilder.build();
  }

  public Optional<Query> buildQueryNode(S searchRequest) {
    return buildQuery(
        searchRequest.getParameters(),
        searchRequest.getQ(),
        searchRequest.isMatchCase(),
        getChecklistKey(searchRequest.getParameters()));
  }

  SearchRequest buildSuggestQuery(String prefix, P parameter, Integer limit, String index) {
    EsField esField = esFieldMapper.getEsField(parameter);
    int suggestLimit = limit != null ? limit : SearchConstants.DEFAULT_SUGGEST_LIMIT;

    return SearchRequest.of(
        sr ->
            sr.index(index)
                .source(src -> src.filter(f -> f.includes(esField.getSearchFieldName())))
                .suggest(
                    su ->
                        su.suggesters(
                            esField.getSearchFieldName(),
                            fs ->
                                fs.prefix(prefix)
                                    .completion(
                                        c ->
                                            c.field(esField.getSuggestFieldName())
                                                .size(suggestLimit)
                                                .skipDuplicates(true)))));
  }

  /**
   * Add a taxon key query to the builder
   *
   * @param rank
   * @param bool
   * @param params
   */
  private void addTaxonKeyQuery(String rank, List<Query> filters, Map<P, Set<String>> params) {

    String checklistKey = getChecklistKey(params);

    P dynamicRankParam = createSearchParam(rank.toUpperCase() + "_KEY", String.class);
    if (params.containsKey(dynamicRankParam)) {
      String checklistField =
          ((ChecklistEsField) OccurrenceEsField.TAXON_KEY.getEsField()).getSearchFieldName(checklistKey);
      List<FieldValue> values =
          params.get(dynamicRankParam).stream().map(FieldValue::of).collect(Collectors.toList());
      Query checklistQuery =
          Query.of(
              q ->
                  q.terms(
                      t ->
                          t.field(checklistField)
                              .terms(v -> v.value(values))));
      params.remove(dynamicRankParam);
      filters.add(checklistQuery);
    }
  }

  @VisibleForTesting
  Optional<Query> buildQuery(
      Map<P, Set<String>> params, String qParam, boolean matchCase, String checklistKey) {
    translateFields(params);
    List<Query> mustQueries = new ArrayList<>();
    List<Query> filterQueries = new ArrayList<>();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      mustQueries.add(buildFullTextQuery(qParam));
    }

    if (params != null && !params.isEmpty()) {

      // check for dynamic ranks e.g. subphylum that will be specific to a checklist
      addChecklistDynamicRanks(params, filterQueries);

      // handle the switch from issue -> non-taxonomic issue if checklistKey supplied
      handleIssueQueries(params, filterQueries);

      // adding geometry to bool
      getParam("GEOMETRY")
          .filter(params::containsKey)
          .ifPresent(
              param -> {
                List<Query> shouldGeometry = params.get(param).stream().map(this::buildGeoShapeQuery).toList();
                filterQueries.add(Query.of(q -> q.bool(b -> b.should(shouldGeometry))));
              });

      getParam("GEO_DISTANCE")
          .filter(params::containsKey)
          .ifPresent(
              param -> {
                List<Query> shouldGeoDistance =
                    params.get(param).stream().map(this::buildGeoDistanceQuery).toList();
                filterQueries.add(Query.of(q -> q.bool(b -> b.should(shouldGeoDistance))));
              });

      // adding term queries to bool
      filterQueries.addAll(createQueries(params, matchCase, checklistKey));
    }
    esFieldMapper.getDefaultFilter().ifPresent(filterQueries::add);

    return mustQueries.isEmpty() && filterQueries.isEmpty()
        ? Optional.empty()
        : Optional.of(Query.of(q -> q.bool(b -> b.must(mustQueries).filter(filterQueries))));
  }

  private List<Query> createQueries(
      Map<P, Set<String>> params, boolean matchCase, String checklistKey) {
    return createQueries(params, matchCase, true, checklistKey);
  }

  private List<Query> createQueries(
      Map<P, Set<String>> params,
      boolean matchCase,
      boolean wrappedChildrenQueries,
      String checklistKey) {
    Map<String, List<Query>> queriesByNestedPath = new HashMap<>();
    List<Query> nonNestedQueries = new ArrayList<>();
    for (Map.Entry<P, Set<String>> e : params.entrySet()) {
      EsField esField = esFieldMapper.getEsField(e.getKey());
      if (esField == null) {
        continue;
      }
      List<Query> queryBuilders =
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

    List<Query> allQueries = new ArrayList<>(nonNestedQueries);

    // add nested queries
    queriesByNestedPath.forEach(
        (key, value) -> {
          allQueries.add(
              Query.of(
                  q ->
                      q.nested(
                          n ->
                              n.path(key)
                                  .query(Query.of(qb -> qb.bool(b -> b.filter(value))))
                                  .scoreMode(ChildScoreMode.None))));
        });

    return allQueries;
  }

  protected abstract void handleIssueQueries(Map<P, Set<String>> params, List<Query> filters);

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
        .orElse(defaultChecklistKey);
  }

  /**
   * Checks the request parameters for any dynamic ranks and adds them to the query e.g.
   * SUBPHYLUM_KEY=XXXXX
   *
   * @param params
   * @param bool
   */
  private void addChecklistDynamicRanks(Map<P, Set<String>> params, List<Query> filters) {
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
                            addTaxonKeyQuery(rank, filters, params);
                          }));
    }
  }

  public Optional<Query> buildQuery(PredicateSearchRequest searchRequest) {
    searchRequest.setPredicate(translatePredicateFields(searchRequest.getPredicate()));
    List<Query> mustQueries = new ArrayList<>();
    String qParam = searchRequest.getQ();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      mustQueries.add(buildFullTextQuery(qParam));
    }

    try {
      esQueryVisitor.getQueryBuilder(searchRequest.getPredicate()).ifPresent(mustQueries::add);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error building predicate query", e);
    }

    return mustQueries.isEmpty()
        ? Optional.empty()
        : Optional.of(Query.of(q -> q.bool(b -> b.must(mustQueries))));
  }

  protected Query buildFullTextQuery(String qParam) {
    return Query.of(q -> q.match(m -> m.field(esFieldMapper.getFullTextField()).query(qParam)));
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

  private Optional<Query> buildPostFilter(
      Map<P, Set<String>> postFilterParams, boolean matchCase, String checklistKey) {
    if (postFilterParams == null || postFilterParams.isEmpty()) {
      return Optional.empty();
    }

    List<Query> filterQueries = createQueries(postFilterParams, matchCase, checklistKey);
    return Optional.of(Query.of(q -> q.bool(b -> b.filter(filterQueries))));
  }

  private Optional<Map<String, Aggregation>> buildAggs(
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
  private Query getAggregationPostFilter(
      Map<P, Set<String>> postFilterParams, P facetParam, boolean matchCase, String checklistKey) {
    Map<P, Set<String>> filteredParams =
        postFilterParams.entrySet().stream()
            .filter(entry -> entry.getKey() != facetParam)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return Query.of(q -> q.bool(b -> b.filter(createQueries(filteredParams, matchCase, checklistKey))));
  }

  /** Creates a filter with all the filter in which the facet param is not present. */
  private Query getAggregationFilter(
      Map<P, Set<String>> postFilterParams,
      P facetParam,
      boolean matchCase,
      boolean wrappedChildrenQueries,
      String checklistKey) {
    return Query.of(
        q ->
            q.bool(
                b ->
                    b.filter(
                        createQueries(
                            Map.of(
                                facetParam,
                                Optional.ofNullable(postFilterParams.get(facetParam))
                                    .orElse(Collections.emptySet())),
                            matchCase,
                            wrappedChildrenQueries,
                            checklistKey))));
  }

  private Map<String, Aggregation> buildFacetsMultiselect(
      S searchRequest, Map<P, Set<String>> postFilterParams) {

    if (searchRequest.getFacets().size() == 1) {
      // same case as normal facets
      return buildFacets(searchRequest);
    }

    Map<String, Aggregation> aggregations = new LinkedHashMap<>();
    searchRequest.getFacets().stream()
        .filter(p -> esFieldMapper.getEsField(p) != null)
        .forEach(
            facetParam -> {
              Query bool =
                  getAggregationPostFilter(
                      postFilterParams,
                      facetParam,
                      searchRequest.isMatchCase(),
                      getChecklistKey(searchRequest.getParameters()));

              EsField esField = esFieldMapper.getEsFacetField(facetParam);
              NamedAggregation subAggregation =
                  esField.isChildField()
                      ? getChildrenAggregationBuilder(
                          searchRequest, postFilterParams, facetParam, esField)
                      : buildTermsAggs(
                          "filtered_" + esField.getSearchFieldName(),
                          esField,
                          searchRequest,
                          facetParam);

              Aggregation filterAggregation =
                  Aggregation.of(
                      a ->
                          a.filter(bool)
                              .aggregations(subAggregation.name, subAggregation.aggregation));
              aggregations.put(esField.getSearchFieldName(), filterAggregation);
            });

    return aggregations;
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

  private Map<String, Aggregation> buildFacets(S searchRequest) {
    final AtomicReference<GroupedParams<P>> groupedParams = new AtomicReference<>();
    Map<String, Aggregation> aggregations = new LinkedHashMap<>();
    searchRequest.getFacets().stream()
        .filter(p -> esFieldMapper.getEsField(p) != null || isDynamicRankParam(searchRequest, p))
        .forEach(
            facetParam -> {
              NamedAggregation aggregation;
              EsField esField = esFieldMapper.getEsFacetField(facetParam);
              if (esField != null && esField.isChildField()) {
                if (groupedParams.get() == null) {
                  groupedParams.set(groupParameters(searchRequest, true));
                }
                aggregation =
                    getChildrenAggregationBuilder(
                    searchRequest, groupedParams.get().postFilterParams, facetParam, esField);
                aggregations.put(aggregation.name, aggregation.aggregation);
                return;
              }

              // if a checklist has been supplied, then use the non taxonomic issues field
              if (getParam("ISSUE").map(facetParam::equals).orElse(false)
                  && getParam("CHECKLIST_KEY")
                      .map(p -> searchRequest.getParameters().containsKey(p))
                      .orElse(false)) {
                aggregation =
                    buildTermsAggs(
                    OccurrenceEsField.NON_TAXONOMIC_ISSUE.getSearchFieldName(),
                    OccurrenceEsField.NON_TAXONOMIC_ISSUE,
                    searchRequest,
                    facetParam);
                aggregations.put(aggregation.name, aggregation.aggregation);
                return;
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
                  aggregation = buildTermsAggs(esFieldName, field, searchRequest, facetParam);
                  aggregations.put(aggregation.name, aggregation.aggregation);
                  return;
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

                aggregation = buildTermsAggs(facetParam.name(), esFieldToUse, searchRequest, facetParam);
                aggregations.put(aggregation.name, aggregation.aggregation);
                return;
              }

              aggregation =
                  buildTermsAggs(
                  esField.getSearchFieldName(), esField, searchRequest, facetParam);
              aggregations.put(aggregation.name, aggregation.aggregation);
            });
    return aggregations;
  }

  private boolean isTaxonomic(P param) {
    return esFieldMapper.isTaxonomic(param);
  }

  private NamedAggregation getChildrenAggregationBuilder(
      S searchRequest, Map<P, Set<String>> postFilterParams, P facetParam, EsField esField) {
    // ES 9 no longer exposes the old ChildrenAggregationBuilder API in this module setup.
    // Use a filter aggregation with a has_child wrapper query to preserve child filtering intent.
    Query childFilter =
        postFilterParams.containsKey(facetParam)
            ? getAggregationFilter(
                postFilterParams,
                facetParam,
                searchRequest.isMatchCase(),
                false,
                getChecklistKey(searchRequest.getParameters()))
            : Query.of(q -> q.matchAll(m -> m));

    NamedAggregation termsAggregation =
        buildTermsAggs(esField.getSearchFieldName(), esField, searchRequest, facetParam);
    Aggregation filterAggregation =
        Aggregation.of(
            a ->
                a.filter(wrapHasChildQuery(esField.childrenRelation(), childFilter))
                    .aggregations(termsAggregation.name, termsAggregation.aggregation));
    return new NamedAggregation(esField.getSearchFieldName(), filterAggregation);
  }

  private NamedAggregation buildTermsAggs(
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

  private NamedAggregation buildTermsAggs(
      String aggName, String fieldName, S searchRequest, P facetParam) {
    return buildTermsAggs(aggName, fieldName, searchRequest, facetParam, null);
  }

  private NamedAggregation buildTermsAggs(
      String aggName, String fieldName, S searchRequest, P facetParam, EsField esField) {
    Integer minCount =
        Optional.ofNullable(searchRequest.getFacetMinCount()).map(Number::intValue).orElse(null);
    int size =
        calculateAggsSize(
            facetParam,
            extractFacetOffset(searchRequest, facetParam),
            extractFacetLimit(searchRequest, facetParam));
    int shardSize = CARDINALITIES.getOrDefault(facetParam, DEFAULT_SHARD_SIZE.applyAsInt(size));
    Aggregation termsAggregation =
        Aggregation.of(
            a ->
                a.terms(
                    t -> {
                      t.field(fieldName).size(size).shardSize(shardSize);
                      if (minCount != null) {
                        t.minDocCount(minCount);
                      }
                      return t;
                    }));

    if (esField != null && esField.isNestedField()) {
      Aggregation nestedAggregation =
          Aggregation.of(
              a ->
                  a.nested(n -> n.path(esField.getNestedPath()))
                      .aggregations(aggName, termsAggregation));
      return new NamedAggregation(facetParam.name(), nestedAggregation);
    } else {
      return new NamedAggregation(aggName, termsAggregation);
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

  private List<Query> buildTermQuery(
      Collection<String> values,
      P param,
      EsField esField,
      boolean matchCase,
      boolean wrappedChildrenQueries,
      String checklistKey) {
    List<Query> queries = new ArrayList<>();

    // collect queries for each value
    List<String> parsedValues = new ArrayList<>();

    List<Query> queryBuilders = new ArrayList<>();
    for (String value : values) {
      if (isNumericRange(value) || esFieldMapper.isDateField(esField)) {
        Query rangeQuery = buildRangeQuery(esField, value);
        queryBuilders.add(rangeQuery);
        if (rangeQuery.isRange() && esFieldMapper.includeNullInRange(param, rangeQuery.range())) {
          queryBuilders.add(
              Query.of(
                  q ->
                      q.bool(
                          b ->
                              b.mustNot(
                                  Query.of(
                                      q2 ->
                                          q2.exists(
                                              e -> e.field(esField.getExactMatchFieldName())))))));
        }
        continue;
      }
      parsedValues.add(parseParamValue(value, param));
    }

    if (!queryBuilders.isEmpty()) {
      queries.add(Query.of(q -> q.bool(b -> b.should(queryBuilders))));
    }

    // get the field name based on the taxonomy and the matchCase
    String fieldName = null;
    if (isTaxonomic(param)) {
      fieldName = esFieldMapper.getChecklistField(checklistKey, param);
    } else {
      fieldName = matchCase ? esField.getVerbatimFieldName() : esField.getExactMatchFieldName();
    }
    final String finalFieldName = fieldName;

    if (parsedValues.size() == 1) {
      // single term
      queries.add(Query.of(q -> q.term(t -> t.field(finalFieldName).value(parsedValues.get(0)))));
    } else if (parsedValues.size() > 1) {
      // multi term query
      queries.add(
          Query.of(
              q ->
                  q.terms(
                      t ->
                          t.field(finalFieldName)
                              .terms(
                                  v ->
                                      v.value(
                                          parsedValues.stream()
                                              .map(FieldValue::of)
                                              .toList())))));
    }

    if (wrappedChildrenQueries && esField.isChildField()) {
      return queries.stream()
          .map(q -> wrapHasChildQuery(esField.childrenRelation(), q))
          .toList();
    }

    return queries;
  }

  private Query buildRangeQuery(EsField esField, String value) {
    String fieldName = esField.getExactMatchFieldName();
    if (esFieldMapper.isDateField(esField)) {
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(value);
      return Query.of(
          q ->
              q.range(
                  r ->
                      r.date(
                          d -> {
                            d.field(fieldName).relation(RangeRelation.Within);
                            if (dateRange.hasLowerBound()) {
                              d.gte(dateRange.lowerEndpoint().toString());
                            }
                            if (dateRange.hasUpperBound()) {
                              d.lt(dateRange.upperEndpoint().toString());
                            }
                            return d;
                          })));
    } else {
      String[] values = value.split(RANGE_SEPARATOR);
      boolean withinGeoTime =
          esField.getSearchFieldName().equals(OccurrenceEsField.GEOLOGICAL_TIME.getSearchFieldName());
      return Query.of(
          q ->
              q.range(
                  r ->
                      r.untyped(
                          u -> {
                            u.field(fieldName);
                            if (!RANGE_WILDCARD.equals(values[0])) {
                              u.gte(JsonData.of(values[0]));
                            }
                            if (!RANGE_WILDCARD.equals(values[1])) {
                              u.lte(JsonData.of(values[1]));
                            }
                            if (withinGeoTime) {
                              u.relation(RangeRelation.Within);
                            }
                            return u;
                          })));
    }
  }

  public Query buildGeoDistanceQuery(String rawGeoDistance) {
    DistanceUnit.GeoDistance geoDistance =
        DistanceUnit.GeoDistance.parseGeoDistance(rawGeoDistance);
    return buildGeoDistanceQuery(geoDistance);
  }

  public Query buildGeoDistanceQuery(DistanceUnit.GeoDistance geoDistance) {
    return Query.of(
        q ->
            q.geoDistance(
                g ->
                    g.field(esFieldMapper.getGeoDistanceEsField().getSearchFieldName())
                        .distance(geoDistance.getDistance().toString())
                        .location(l -> l.latlon(ll -> ll.lat(geoDistance.getLatitude()).lon(geoDistance.getLongitude())))));
  }

  public Query buildGeoShapeQuery(String wkt) {
    Geometry geometry;
    try {
      geometry = new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    String geoJson = geometryToGeoJson(geometry);
    String queryJson =
        "{\"geo_shape\":{\""
            + esFieldMapper.getGeoShapeEsField().getSearchFieldName()
            + "\":{\"shape\":"
            + geoJson
            + ",\"relation\":\"within\"}}}";
    return Query.of(
        q ->
            q.withJson(
                new ByteArrayInputStream(
                    queryJson.getBytes(StandardCharsets.UTF_8))));
  }

  private Query wrapHasChildQuery(String relation, Query query) {
    return Query.of(
        q ->
            q.hasChild(
                h ->
                    h.type(relation)
                        .query(query)
                        .scoreMode(ChildScoreMode.None)));
  }

  private String geometryToGeoJson(Geometry geometry) {
    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LineString"
            : geometry.getGeometryType();

    if ("Point".equals(type)) {
      Coordinate c = geometry.getCoordinate();
      return "{\"type\":\"Point\",\"coordinates\":[" + c.x + "," + c.y + "]}";
    }

    if ("LineString".equals(type)) {
      return "{\"type\":\"LineString\",\"coordinates\":" + coordinatesToJson(geometry.getCoordinates()) + "}";
    }

    if ("Polygon".equals(type)) {
      return polygonToGeoJson((Polygon) geometry);
    }

    if ("MultiPolygon".equals(type)) {
      StringBuilder sb = new StringBuilder();
      sb.append("{\"type\":\"MultiPolygon\",\"coordinates\":[");
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        String poly = polygonToGeoJson((Polygon) geometry.getGeometryN(i));
        int start = poly.indexOf("[");
        int end = poly.lastIndexOf("]");
        sb.append(poly, start, end + 1);
      }
      sb.append("]}");
      return sb.toString();
    }

    throw new IllegalArgumentException(type + " shape is not supported");
  }

  private String polygonToGeoJson(Polygon polygon) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"type\":\"Polygon\",\"coordinates\":[");
    sb.append(coordinatesToJson(normalizePolygonCoordinates(polygon.getExteriorRing().getCoordinates())));
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      sb.append(",");
      sb.append(
          coordinatesToJson(
              normalizePolygonCoordinates(polygon.getInteriorRingN(i).getCoordinates())));
    }
    sb.append("]}");
    return sb.toString();
  }

  private String coordinatesToJson(Coordinate[] coordinates) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < coordinates.length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("[").append(coordinates[i].x).append(",").append(coordinates[i].y).append("]");
    }
    sb.append("]");
    return sb.toString();
  }

  protected abstract Optional<P> getParam(String name);

  protected abstract P createSearchParam(String name, Class<?> type);

  protected abstract void translateFields(Map<P, Set<String>> params);
  protected abstract Predicate translatePredicateFields(Predicate predicate);

  protected void handleOccurrenceIssueQueries(
      Map<OccurrenceSearchParameter, Set<String>> params, List<Query> filters) {
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

      List<FieldValue> issueValues =
          params.get(OccurrenceSearchParameter.ISSUE).stream()
              .map(FieldValue::of)
              .toList();
      filters.add(
          Query.of(
              q ->
                  q.terms(
                      t ->
                          t.field(esFieldToUse)
                              .terms(v -> v.value(issueValues)))));
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

  private static class NamedAggregation {
    final String name;
    final Aggregation aggregation;

    private NamedAggregation(String name, Aggregation aggregation) {
      this.name = name;
      this.aggregation = aggregation;
    }
  }
}
