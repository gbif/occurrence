package org.gbif.occurrence.download.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.IsNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;

import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.search.es.EsQueryUtils;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.OccurrenceEsField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class translates predicates in GBIF download API to equivalent Elastic Search query requests.
 * The json string provided by {@link #getQuery(Predicate)} can be used with _search get requests of ES index to produce downloads.
 */
public class EsQueryVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(EsQueryVisitor.class);


  private static OccurrenceEsField getElasticField(OccurrenceSearchParameter param) {
    return EsQueryUtils.SEARCH_TO_ES_MAPPING.get(param);
  }

  private static String getElasticFieldName(OccurrenceSearchParameter param) {
    return EsQueryUtils.SEARCH_TO_ES_MAPPING.get(param).getExactMatchFieldName();
  }

  private static String getExactMatchOrVerbatimField(SimplePredicate predicate) {
    OccurrenceEsField esField = getElasticField(predicate.getKey());
    return predicate.isMatchCase()? esField.getVerbatimFieldName() : esField.getExactMatchFieldName();
  }

  private static String getExactMatchOrVerbatimField(InPredicate predicate) {
    OccurrenceEsField esField = getElasticField(predicate.getKey());
    return Optional.ofNullable(predicate.isMatchCase()).orElse(Boolean.FALSE)? esField.getVerbatimFieldName() : esField.getExactMatchFieldName();
  }

  private static String parseParamValue(String value, OccurrenceSearchParameter parameter) {
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

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
   * json query that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   *
   * @return body clause
   */
  public String getQuery(Predicate predicate) throws QueryBuildingException {
    if (predicate != null) {
      BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
      visit(predicate, queryBuilder);
      return queryBuilder.toString();
    } else {
      return QueryBuilders.matchAllQuery().toString();
    }
  }

  /**
   * handle conjunction predicate
   *
   * @param predicate conjunction predicate
   * @param queryBuilder  root query builder
   */
  public void visit(ConjunctionPredicate predicate, BoolQueryBuilder queryBuilder) throws QueryBuildingException {
    // must query structure is equivalent to AND
    predicate.getPredicates().forEach(subPredicate -> {
      try {
        BoolQueryBuilder mustQueryBuilder = QueryBuilders.boolQuery();
        visit(subPredicate, mustQueryBuilder);
        queryBuilder.filter(mustQueryBuilder);
      } catch (QueryBuildingException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  /**
   * handle disjunction predicate
   *
   * @param predicate disjunction predicate
   * @param queryBuilder  root query builder
   */
  public void visit(DisjunctionPredicate predicate, BoolQueryBuilder queryBuilder) throws QueryBuildingException {
    Map<OccurrenceSearchParameter, List<EqualsPredicate>> equalsPredicatesReplaceableByIn = groupEquals(predicate);

    predicate.getPredicates().forEach(subPredicate -> {
      try {
        if (!isReplaceableByInPredicate(subPredicate, equalsPredicatesReplaceableByIn)) {
          BoolQueryBuilder shouldQueryBuilder = QueryBuilders.boolQuery();
          visit(subPredicate, shouldQueryBuilder);
          queryBuilder.should(shouldQueryBuilder);
        }
      } catch (QueryBuildingException ex) {
        throw new RuntimeException(ex);
      }
    });
    if (!equalsPredicatesReplaceableByIn.isEmpty()) {
      toInPredicates(equalsPredicatesReplaceableByIn)
        .forEach(ep -> queryBuilder.should().add(QueryBuilders.termsQuery(getExactMatchOrVerbatimField(ep),
                                                                          ep.getValues().stream()
                                                                            .map(v -> parseParamValue(v, ep.getKey()))
                                                                            .collect(Collectors.toList()))));
    }
  }

  /**
   * Checks if a predicate has in grouped and can be replaced later by a InPredicate.
   */
  private boolean isReplaceableByInPredicate(Predicate predicate, Map<OccurrenceSearchParameter, List<EqualsPredicate>> equalsPredicatesReplaceableByIn) {
    if (!equalsPredicatesReplaceableByIn.isEmpty() && predicate instanceof EqualsPredicate) {
      EqualsPredicate equalsPredicate = (EqualsPredicate)predicate;
      return equalsPredicatesReplaceableByIn.containsKey(equalsPredicate.getKey()) && equalsPredicatesReplaceableByIn.get(equalsPredicate.getKey()).contains(equalsPredicate);
    }
    return false;
  }

  /**
   * Groups all equals predicates by search parameter.
   */
  private static Map<OccurrenceSearchParameter, List<EqualsPredicate>> groupEquals(DisjunctionPredicate predicate) {
    return predicate.getPredicates().stream()
            .filter(p -> p instanceof EqualsPredicate)
            .map(p -> (EqualsPredicate)p)
            .collect(Collectors.groupingBy(EqualsPredicate::getKey))
            .entrySet().stream()
            .filter( e -> e.getValue().size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Transforms the grouped EqualsPredicates into InPredicates.
   */
  private List<InPredicate> toInPredicates(Map<OccurrenceSearchParameter, List<EqualsPredicate>> equalPredicates) {
    return equalPredicates.entrySet()
            .stream()
            .map(e ->
              e.getValue().stream()
                .collect(Collectors.groupingBy(EqualsPredicate::isMatchCase))
                .entrySet()
                .stream()
                .map(group -> new InPredicate(e.getKey(), group.getValue().stream().map(EqualsPredicate::getValue).collect(Collectors.toSet()), group.getKey()))
                 .collect(Collectors.toList())
            )
            .flatMap(List::stream)
            .collect(Collectors.toList());
  }

  /**
   * handles EqualPredicate
   *
   * @param predicate equalPredicate
   */
  public void visit(EqualsPredicate predicate, BoolQueryBuilder queryBuilder) {
    OccurrenceSearchParameter parameter = predicate.getKey();
    queryBuilder.filter().add(QueryBuilders.termQuery(getExactMatchOrVerbatimField(predicate), parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handle greater than equals predicate
   *
   * @param predicate gte predicate
   * @param queryBuilder  root query builder
   */
  public void visit(GreaterThanOrEqualsPredicate predicate, BoolQueryBuilder queryBuilder) {
    OccurrenceSearchParameter parameter = predicate.getKey();
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticFieldName(parameter)).gte(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handles greater than predicate
   *
   * @param predicate greater than predicate
   * @param queryBuilder  root query builder
   */
  public void visit(GreaterThanPredicate predicate, BoolQueryBuilder queryBuilder) {
    OccurrenceSearchParameter parameter = predicate.getKey();
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticFieldName(parameter)).gt(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   * @param queryBuilder  root query builder
   */
  public void visit(InPredicate predicate, BoolQueryBuilder queryBuilder) {
    OccurrenceSearchParameter parameter = predicate.getKey();
    queryBuilder.filter().add(QueryBuilders.termsQuery(getExactMatchOrVerbatimField(predicate),
                                                       predicate.getValues().stream()
                                                         .map(v -> parseParamValue(v, parameter))
                                                         .collect(Collectors.toList())));
  }

  /**
   * handles less than or equals predicate
   *
   * @param predicate less than or equals
   * @param queryBuilder  root query builder
   */
  public void visit(LessThanOrEqualsPredicate predicate, BoolQueryBuilder queryBuilder) {
    OccurrenceSearchParameter parameter = predicate.getKey();
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticFieldName(parameter))
                                .lte(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handles less than predicate
   *
   * @param predicate less than predicate
   * @param queryBuilder  root query builder
   */
  public void visit(LessThanPredicate predicate, BoolQueryBuilder queryBuilder) {
    OccurrenceSearchParameter parameter = predicate.getKey();
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticFieldName(parameter))
                                .lt(parseParamValue(predicate.getValue(), parameter)));
  }

  /**
   * handles like predicate
   *
   * @param predicate like predicate
   * @param queryBuilder  root query builder
   */
  public void visit(LikePredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.wildcardQuery(getExactMatchOrVerbatimField(predicate),
                                                          predicate.getValue()));
  }

  /**
   * handles not predicate
   *
   * @param predicate NOT predicate
   * @param queryBuilder  root query builder
   */
  public void visit(NotPredicate predicate, BoolQueryBuilder queryBuilder) throws QueryBuildingException {
    BoolQueryBuilder mustNotQueryBuilder = QueryBuilders.boolQuery();
    visit(predicate.getPredicate(), mustNotQueryBuilder);
    queryBuilder.mustNot(mustNotQueryBuilder);
  }

  /**
   * handles within predicate
   *
   * @param within   Within predicate
   * @param queryBuilder root query builder
   */
  public void visit(WithinPredicate within, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter(EsSearchRequestBuilder.buildGeoShapeQuery(within.getGeometry()));
  }

  /**
   * handles ISNOTNULL Predicate
   *
   * @param predicate ISNOTNULL predicate
   */
  public void visit(IsNotNullPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.existsQuery(getElasticFieldName(predicate.getParameter())));
  }

  /**
   * handles ISNULL Predicate
   *
   * @param predicate ISNULL predicate
   */
  public void visit(IsNullPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.boolQuery()
                                .mustNot(QueryBuilders.existsQuery(getElasticFieldName(predicate.getParameter()))));
  }

  private void visit(Object object, BoolQueryBuilder queryBuilder) throws QueryBuildingException {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass(), BoolQueryBuilder.class);
    } catch (NoSuchMethodException e) {
      LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object, queryBuilder);
    } catch (IllegalAccessException e) {
      LOG.error("This error shouldn't occur if all visit methods are public. Probably a programming error", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the query", e);
      throw new QueryBuildingException(e);
    }
  }

}
