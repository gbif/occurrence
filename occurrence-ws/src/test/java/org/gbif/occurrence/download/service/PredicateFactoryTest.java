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
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.Predicate;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PredicateFactoryTest {

  @Test
  public void testWithinPredicateValidation() {

    // Valid predicate should pass
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.GEOMETRY.name(), new String[] {"POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"});

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

  @Test
  public void testInChecklistKeyPredicate() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.CHECKLIST_KEY.name(), new String[] {"7ddf754f-d193-4cc9-b351-99906754a03b"});
    params.put(OccurrenceSearchParameter.SPECIES_KEY.name(), new String[] {"5XCPW"});

    Predicate predicate = PredicateFactory.build(params);

    assertNotNull(predicate);
  }

  @Test
  public void testDnaTaxonSearchOnTaxonomicPredicates() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.TAXON_KEY.name(), new String[] {"212"});
    params.put(OccurrenceSearchParameter.KINGDOM_KEY.name(), new String[] {"1", "6"});
    params.put(OccurrenceSearchParameter.COUNTRY.name(), new String[] {"DK"});
    params.put("dnaTaxonSearch", new String[] {"true"});

    ConjunctionPredicate predicate = (ConjunctionPredicate) PredicateFactory.build(params);

    for (Predicate p : predicate.getPredicates()) {
      if (p instanceof EqualsPredicate<?> equals) {
        if (equals.getKey() == OccurrenceSearchParameter.TAXON_KEY) {
          assertEquals(Boolean.TRUE, equals.getDnaTaxonSearch());
        } else {
          // it only applies to the taxonomic parameters
          assertEquals(OccurrenceSearchParameter.COUNTRY, equals.getKey());
          assertNull(equals.getDnaTaxonSearch());
        }
      } else {
        InPredicate<?> in = (InPredicate<?>) p;
        assertEquals(OccurrenceSearchParameter.KINGDOM_KEY, in.getKey());
        assertEquals(Boolean.TRUE, in.getDnaTaxonSearch());
      }
    }
  }

  @Test
  public void testWithoutDnaTaxonSearch() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.TAXON_KEY.name(), new String[] {"212"});
    params.put("dnaTaxonSearch", new String[] {"false"});

    EqualsPredicate<?> predicate = (EqualsPredicate<?>) PredicateFactory.build(params);

    assertNull(predicate.getDnaTaxonSearch());
  }
}
