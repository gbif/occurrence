package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PredicateOptimizer {

  private static Logger LOG = LoggerFactory.getLogger(PredicateOptimizer.class);

  private PredicateOptimizer() {

  }

  public static Predicate optimize(Predicate predicate) {
    Object v = new PredicateOptimizer().visit(predicate);
    return v instanceof Predicate ? (Predicate) v : predicate;
  }

  /**
   * handle conjunction predicate
   *
   * @param predicate conjunction predicate
   */
  public void visit(ConjunctionPredicate predicate) {
    // must query structure is equivalent to AND
    predicate.getPredicates().forEach(this::visit);
  }


  public Predicate visit(DisjunctionPredicate predicate) {
    Map<OccurrenceSearchParameter, List<EqualsPredicate>> equalsPredicates = groupEqualsPredicate(predicate);
    if (!equalsPredicates.isEmpty()) {
      List<Predicate> predicates = new ArrayList<>(predicate.getPredicates());
      List<Predicate> exclude = predicates.stream()
        .filter(p -> isReplaceableByInPredicate(p, equalsPredicates))
        .collect(Collectors.toList());
      predicates.removeAll(exclude);
      predicates.addAll(toInPredicates(equalsPredicates));
      return new DisjunctionPredicate(predicates);
    }
    return predicate;
  }

  /**
   * Checks if a predicate has in grouped and can be replaced later by a InPredicate.
   */
  private static boolean isReplaceableByInPredicate(Predicate predicate, Map<OccurrenceSearchParameter, List<EqualsPredicate>> equalsPredicatesReplaceableByIn) {
    if (!equalsPredicatesReplaceableByIn.isEmpty() && predicate instanceof EqualsPredicate) {
      EqualsPredicate equalsPredicate = (EqualsPredicate)predicate;
      return equalsPredicatesReplaceableByIn.containsKey(equalsPredicate.getKey()) && equalsPredicatesReplaceableByIn.get(equalsPredicate.getKey()).contains(equalsPredicate);
    }
    return false;
  }

  /**
   * Groups all equals predicates by search parameter.
   */
  private static Map<OccurrenceSearchParameter, List<EqualsPredicate>> groupEqualsPredicate(DisjunctionPredicate predicate) {
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
  private static List<InPredicate> toInPredicates(Map<OccurrenceSearchParameter, List<EqualsPredicate>> equalPredicates) {
    return equalPredicates.entrySet()
      .stream()
      .map(e -> new InPredicate(e.getKey(), e.getValue().stream().map(EqualsPredicate::getValue).collect(Collectors.toSet())))
      .collect(Collectors.toList());
  }


  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   */
  public void visit(InPredicate predicate) {
    return;
  }


  /**
   * handles like predicate
   *
   * @param predicate like predicate
   */
  public void visit(SimplePredicate predicate) {
    return;
  }

  /**
   * handles not predicate
   *
   * @param predicate NOT predicate
   * @param queryBuilder  root query builder
   */
  public void visit(NotPredicate predicate) {
    return;
  }

  /**
   * handles within predicate
   *
   * @param within   Within predicate
   * @param queryBuilder toor query builder
   */
  public void visit(WithinPredicate within) {
    return;
  }

  /**
   * handles EqualsPredicate Predicate
   *
   * @param predicate EqualsPredicate predicate
   */
  public void visit(EqualsPredicate predicate) {
    return;
  }

  /**
   * handles ISNOTNULL Predicate
   *
   * @param predicate ISNOTNULL predicate
   */
  public void visit(IsNotNullPredicate predicate) {
    return;
  }

  private Object visit(Object object) {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass());
    } catch (NoSuchMethodException e) {
      LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      return method.invoke(this, object);
    } catch (IllegalAccessException e) {
      LOG.error("This error shouldn't occur if all visit methods are public. Probably a programming error", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the query", e);
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Exception thrown while building the query");
  }
}
