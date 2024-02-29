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
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.SearchTypeValidator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Provides additional validations not captured by GBIF API classes.
 * Only the Within predicate is considered by this validator.
 */
public class PredicateValidator {

  private static Logger LOG = LoggerFactory.getLogger(PredicateValidator.class);

  private PredicateValidator() {

  }

  public static void validate(Predicate predicate) {
    if (Objects.nonNull(predicate)) {
      new PredicateValidator().visit(predicate);
    }
  }

  /**
   * handle conjunction predicate
   */
  public void visit(ConjunctionPredicate predicate) {
    predicate.getPredicates().forEach(this::visit);
  }

  /**
   * handle disjunction predicate
   */
  public void visit(DisjunctionPredicate predicate) {
    predicate.getPredicates().forEach(this::visit);
  }

  /**
   * handles EqualPredicate
   */
  public void visit(EqualsPredicate predicate) {
    return;
  }

  /**
   * handle greater than equals predicate
   */
  public void visit(GreaterThanOrEqualsPredicate predicate) {
    return;
  }

  /**
   * handles greater than predicate
   */
  public void visit(GreaterThanPredicate predicate) {
    return;
  }

  /**
   * handle IN Predicate
   */
  public void visit(InPredicate predicate) {
    return;
  }

  /**
   * handles less than or equals predicate
   */
  public void visit(LessThanOrEqualsPredicate predicate) {
    return;
  }

  /**
   * handles less than predicate
   */
  public void visit(LessThanPredicate predicate) {
    return;
  }

  /**
   * handles like predicate
   */
  public void visit(LikePredicate predicate) {
    return;
  }

  /**
   * handles not predicate
   */
  public void visit(NotPredicate predicate) {
    return;
  }

  /**
   * handles within predicate, validates the Geometry.
   */
  public void visit(WithinPredicate within) {
    SearchTypeValidator.validate(OccurrenceSearchParameter.GEOMETRY, within.getGeometry());
  }

  /**
   * handles GeoDistance predicate.
   */
  public void visit(GeoDistancePredicate geoDistance) {
    return;
  }

  /**
   * handles IsNotNullPredicate Predicate
   */
  public void visit(IsNotNullPredicate predicate) {
    return;
  }

  /**
   * handles IsNullPredicate Predicate
   */
  public void visit(IsNullPredicate predicate) {
    return;
  }

  private void visit(Object object) {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass());
    } catch (NoSuchMethodException e) {
      LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object);
    } catch (IllegalAccessException e) {
      LOG.error("This error shouldn't occur if all visit methods are public. Probably a programming error", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the query", e);
      Throwables.propagate(e.getTargetException());
    }
  }
}
