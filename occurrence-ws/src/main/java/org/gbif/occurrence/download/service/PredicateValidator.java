package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GeoDistancePredicate;
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
import org.gbif.api.util.SearchTypeValidator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * handles within predicate, validates the Geometry.
   */
  public void visit(GeoDistancePredicate geoDistance) {
    SearchTypeValidator.validate(OccurrenceSearchParameter.GEO_DISTANCE, geoDistance.getGeoDistance().toGeoDistanceString());
  }


  /**
   * handles IsNotNullPredicate Predicate
   */
  public void visit(IsNotNullPredicate predicate) {
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
