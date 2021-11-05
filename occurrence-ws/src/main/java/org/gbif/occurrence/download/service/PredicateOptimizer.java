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
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.predicate.*;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class PredicateOptimizer {

  private static Logger LOG = LoggerFactory.getLogger(PredicateOptimizer.class);

  private PredicateOptimizer() {

  }

  public static Predicate optimize(Predicate predicate) {
    if (Objects.nonNull(predicate)) {
      Object v = new PredicateOptimizer().visit(predicate);
      return v instanceof Predicate ? (Predicate) v : predicate;
    }
    return predicate;
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
      .map(e -> new InPredicate(e.getKey(), e.getValue().stream().map(EqualsPredicate::getValue).collect(Collectors.toSet()), false))
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
   * @param queryBuilder toor query builderCompoundPredicate
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
   * handles IsNotNull Predicate
   *
   * @param predicate IsNotNull predicate
   */
  public void visit(IsNotNullPredicate predicate) {
    return;
  }

  /**
   * handles IsNull Predicate
   *
   * @param predicate IsNull predicate
   */
  public void visit(IsNullPredicate predicate) {
    return;
  }

  /**
   * handles GreaterThanOrEqualsPredicate Predicate
   *
   * @param predicate GreaterThanOrEqualsPredicate predicate
   */
  public void visit(GreaterThanOrEqualsPredicate predicate) {
    return;
  }

  /**
   * handles GreaterThanPredicate Predicate
   *
   * @param predicate GreaterThanPredicate predicate
   */
  public void visit(GreaterThanPredicate predicate) {
    return;
  }

  /**
   * handles LessThanOrEqualsPredicate Predicate
   *
   * @param predicate LessThanOrEqualsPredicate predicate
   */
  public void visit(LessThanOrEqualsPredicate predicate) {
    return;
  }

  /**
   * handles LessThanPredicate Predicate
   *
   * @param predicate LessThanPredicate predicate
   */
  public void visit(LessThanPredicate predicate) {
    return;
  }

  /**
   * handles LikePredicate Predicate
   *
   * @param predicate LikePredicate predicate
   */
  public void visit(LikePredicate predicate) {
    return;
  }

  /**
   * handles CompoundPredicate Predicate
   *
   * @param predicate CompoundPredicate predicate
   */
  public void visit(CompoundPredicate predicate) {
    return;
  }

  /**
   * handles GeoDistancePredicate Predicate
   *
   * @param predicate GeoDistancePredicate predicate
   */
  public void visit(GeoDistancePredicate predicate) {
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
