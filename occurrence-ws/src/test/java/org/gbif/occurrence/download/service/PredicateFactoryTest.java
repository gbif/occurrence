package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PredicateFactoryTest {

  @Test
  public void testWithinPredicateValidation() {

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      // Valid predicate should pass
      Map<String, String[]> params = new HashMap<>();
      params.put(OccurrenceSearchParameter.GEOMETRY.name(), new String[] {"POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))"});

      PredicateFactory.build(params);

      // Invalid predicate should fail
      params.clear();
      params.put(OccurrenceSearchParameter.GEOMETRY.name(), new String[] {"POLYGON ((30 10 10))"});

      PredicateFactory.build(params);
    });
  }
}
