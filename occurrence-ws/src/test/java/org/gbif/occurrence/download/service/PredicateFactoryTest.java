package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PredicateFactoryTest {

  @Test
  public void testWithinPredicateValidation() {

    // Valid predicate should pass
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.GEOMETRY.name(), new String[] {"POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))"});

    assertNotNull(PredicateFactory.build(params));

    assertThrows(IllegalArgumentException.class, () -> {
      // Invalid predicate should fail
      params.clear();
      params.put(OccurrenceSearchParameter.GEOMETRY.name(), new String[] {"POLYGON ((30 10 10))"});

      PredicateFactory.build(params);
    });
  }

  @Test
  public void testGeoDistancePredicateValidation() {

    // Valid predicate should pass
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.GEO_DISTANCE.name(), new String[] {"30,10,10km"});

    assertNotNull(PredicateFactory.build(params));

    assertThrows(IllegalArgumentException.class, () -> {
      // Invalid predicate should fail
      params.clear();
      params.put(OccurrenceSearchParameter.GEO_DISTANCE.name(), new String[] {"3,10,10we"});

      PredicateFactory.build(params);
    });
  }
}
