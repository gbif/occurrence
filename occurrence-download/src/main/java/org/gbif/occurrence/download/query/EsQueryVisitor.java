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
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import org.gbif.occurrence.search.es.EsQueryUtils;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class translates predicates in GBIF download API to equivalent Elastic Search query requests.
 * The json string provided by {@link #getQuery(Predicate)} can be used with _search get requests of ES index to produce downloads.
 */
public class EsQueryVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(EsQueryVisitor.class);


  private static String getElasticField(OccurrenceSearchParameter param) {
    return EsQueryUtils.SEARCH_TO_ES_MAPPING.get(param).getFieldName();
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
   * json query that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   *
   * @return body clause
   */
  public synchronized String getQuery(Predicate predicate) throws QueryBuildingException {
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
        queryBuilder.must(mustQueryBuilder);
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
      toInPredicates(equalsPredicatesReplaceableByIn).forEach(ep -> queryBuilder.should().add(QueryBuilders.termsQuery(getElasticField(ep.getKey()), ep.getValues())));
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
            .map(e -> new InPredicate(e.getKey(), e.getValue().stream().map(EqualsPredicate::getValue).collect(Collectors.toSet())))
            .collect(Collectors.toList());
  }



  /**
   * handles EqualPredicate
   *
   * @param predicate equalPredicate
   */
  public void visit(EqualsPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.matchQuery(getElasticField(predicate.getKey()), predicate.getValue()));
  }

  /**
   * handle greater than equals predicate
   *
   * @param predicate gte predicate
   * @param queryBuilder  root query builder
   */
  public void visit(GreaterThanOrEqualsPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticField(predicate.getKey())).gte(predicate.getValue()));
  }

  /**
   * handles greater than predicate
   *
   * @param predicate greater than predicate
   * @param queryBuilder  root query builder
   */
  public void visit(GreaterThanPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticField(predicate.getKey())).gt(predicate.getValue()));
  }

  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   * @param queryBuilder  root query builder
   */
  public void visit(InPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.termsQuery(getElasticField(predicate.getKey()), predicate.getValues()));
  }

  /**
   * handles less than or equals predicate
   *
   * @param predicate less than or equals
   * @param queryBuilder  root query builder
   */
  public void visit(LessThanOrEqualsPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticField(predicate.getKey())).lte(predicate.getValue()));
  }

  /**
   * handles less than predicate
   *
   * @param predicate less than predicate
   * @param queryBuilder  root query builder
   */
  public void visit(LessThanPredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.rangeQuery(getElasticField(predicate.getKey())).lt(predicate.getValue()));
  }

  /**
   * handles like predicate
   *
   * @param predicate like predicate
   * @param queryBuilder  root query builder
   */
  public void visit(LikePredicate predicate, BoolQueryBuilder queryBuilder) {
    queryBuilder.filter().add(QueryBuilders.wildcardQuery(getElasticField(predicate.getKey()), predicate.getValue() + "*"));
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
   * @param queryBuilder toor query builder
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
    queryBuilder.filter().add(QueryBuilders.existsQuery(getElasticField(predicate.getParameter())));
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
