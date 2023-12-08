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

  @Test
  public void testInPredicateValidation() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.OCCURRENCE_STATUS.name(), new String[] {"present"});
    params.put(OccurrenceSearchParameter.CATALOG_NUMBER.name(), new String[] {"A", "B", "C"});
    params.put(OccurrenceSearchParameter.YEAR.name(), new String[] {"*,1980", "1990", "2000,2010"});
    params.put(OccurrenceSearchParameter.EVENT_DATE.name(), new String[] {"*,1980", "1990", "2000,2010", "2023-09"});

    assertNotNull(PredicateFactory.build(params));
  }
}
