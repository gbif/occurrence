package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PredicateValidatorTest {


  @Test
  public void invalidPolygonTest() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      //Invalid polygon
      WithinPredicate withinPredicate = new WithinPredicate("POLYGON ((30 10 10))");

      //Valid depth
      EqualsPredicate equalsPredicate = new EqualsPredicate(OccurrenceSearchParameter.DEPTH, "10", true);

      //Conjunction predicate
      ConjunctionPredicate conjunctionPredicate = new ConjunctionPredicate(Arrays.asList(withinPredicate, equalsPredicate));

      //Test
      PredicateValidator.validate(conjunctionPredicate);
    });
  }

  @Test
  public void validPolygonTest() {

      //Valid polygon
      WithinPredicate withinPredicate = new WithinPredicate("POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))");

      //Valid depth
      EqualsPredicate equalsPredicate = new EqualsPredicate(OccurrenceSearchParameter.DEPTH, "10", true);

      //Conjunction predicate
      ConjunctionPredicate conjunctionPredicate = new ConjunctionPredicate(Arrays.asList(withinPredicate, equalsPredicate));

      //Test
      PredicateValidator.validate(conjunctionPredicate);

  }
}
