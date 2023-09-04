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
package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.CompoundPredicate;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GeoDistancePredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.IsNullPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.LessThanPredicate;
import org.gbif.api.model.predicate.LikePredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.SimplePredicate;
import org.gbif.api.model.predicate.WithinPredicate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PredicateOptimizer {

  public static Predicate optimize(Predicate predicate) {
    if (predicate != null) {
      Object v = new PredicateOptimizer().visit(predicate);
      return v instanceof Predicate ? (Predicate) v : predicate;
    }
    return predicate;
  }

  /** Checks if a predicate has in grouped and can be replaced later by a InPredicate. */
  private static boolean isReplaceableByInPredicate(
      Predicate predicate,
      Map<SearchParameter, List<EqualsPredicate>> equalsPredicatesReplaceableByIn) {
    if (!equalsPredicatesReplaceableByIn.isEmpty() && predicate instanceof EqualsPredicate) {
      EqualsPredicate equalsPredicate = (EqualsPredicate) predicate;
      return equalsPredicatesReplaceableByIn.containsKey(equalsPredicate.getKey())
          && equalsPredicatesReplaceableByIn
              .get(equalsPredicate.getKey())
              .contains(equalsPredicate);
    }
    return false;
  }

  /** Groups all equals predicates by search parameter. */
  private static Map<SearchParameter, List<EqualsPredicate>> groupEqualsPredicate(
      DisjunctionPredicate predicate) {
    return predicate.getPredicates().stream()
        .filter(EqualsPredicate.class::isInstance)
        .map(p -> (EqualsPredicate) p)
        .collect(Collectors.groupingBy(EqualsPredicate::getKey))
        .entrySet()
        .stream()
        .filter(e -> e.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Transforms the grouped EqualsPredicates into InPredicates. */
  private static List<InPredicate> toInPredicates(
      Map<SearchParameter, List<EqualsPredicate>> equalPredicates) {
    return equalPredicates.entrySet().stream()
        .map(
            e ->
                new InPredicate(
                    e.getKey(),
                    e.getValue().stream()
                        .map(EqualsPredicate::getValue)
                        .collect(Collectors.toSet()),
                    false))
        .collect(Collectors.toList());
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
    Map<SearchParameter, List<EqualsPredicate>> equalsPredicates =
        groupEqualsPredicate(predicate);
    if (equalsPredicates.isEmpty()) {
      return predicate;
    }
    List<Predicate> predicates = new ArrayList<>(predicate.getPredicates());
    List<Predicate> exclude =
        predicates.stream()
            .filter(p -> isReplaceableByInPredicate(p, equalsPredicates))
            .collect(Collectors.toList());
    predicates.removeAll(exclude);
    predicates.addAll(toInPredicates(equalsPredicates));
    return new DisjunctionPredicate(predicates);
  }

  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   */
  public void visit(InPredicate predicate) {}

  /**
   * handles like predicate
   *
   * @param predicate like predicate
   */
  public void visit(SimplePredicate predicate) {}

  /**
   * handles not predicate
   *
   * @param predicate NOT predicate
   */
  public void visit(NotPredicate predicate) {}

  /**
   * handles within predicate
   *
   * @param within Within predicate
   */
  public void visit(WithinPredicate within) {}

  /**
   * handles EqualsPredicate Predicate
   *
   * @param predicate EqualsPredicate predicate
   */
  public void visit(EqualsPredicate predicate) {}

  /**
   * handles IsNotNull Predicate
   *
   * @param predicate IsNotNull predicate
   */
  public void visit(IsNotNullPredicate predicate) {}

  /**
   * handles IsNull Predicate
   *
   * @param predicate IsNull predicate
   */
  public void visit(IsNullPredicate predicate) {}

  /**
   * handles GreaterThanOrEqualsPredicate Predicate
   *
   * @param predicate GreaterThanOrEqualsPredicate predicate
   */
  public void visit(GreaterThanOrEqualsPredicate predicate) {}

  /**
   * handles GreaterThanPredicate Predicate
   *
   * @param predicate GreaterThanPredicate predicate
   */
  public void visit(GreaterThanPredicate predicate) {}

  /**
   * handles LessThanOrEqualsPredicate Predicate
   *
   * @param predicate LessThanOrEqualsPredicate predicate
   */
  public void visit(LessThanOrEqualsPredicate predicate) {}

  /**
   * handles LessThanPredicate Predicate
   *
   * @param predicate LessThanPredicate predicate
   */
  public void visit(LessThanPredicate predicate) {}

  /**
   * handles LikePredicate Predicate
   *
   * @param predicate LikePredicate predicate
   */
  public void visit(LikePredicate predicate) {}

  /**
   * handles CompoundPredicate Predicate
   *
   * @param predicate CompoundPredicate predicate
   */
  public void visit(CompoundPredicate predicate) {}

  /**
   * handles GeoDistancePredicate Predicate
   *
   * @param predicate GeoDistancePredicate predicate
   */
  public void visit(GeoDistancePredicate predicate) {}

  @SneakyThrows
  private Object visit(Object object) {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass());
    } catch (NoSuchMethodException e) {
      log.warn(
          "Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      return method.invoke(this, object);
    } catch (IllegalAccessException e) {
      log.error(
          "This error shouldn't occur if all visit methods are public. Probably a programming error",
          e);
      throw e;
    } catch (InvocationTargetException e) {
      log.info("Exception thrown while building the query", e);
      throw e;
    }
  }
}
