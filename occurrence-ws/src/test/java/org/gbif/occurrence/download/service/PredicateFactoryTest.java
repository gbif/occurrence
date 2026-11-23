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
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.search.es.occurrence.OccurrenceEsField;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  public void testUseDnaAssistanceOnTaxonomicPredicates() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.TAXON_KEY.name(), new String[] {"212"});
    params.put(OccurrenceSearchParameter.KINGDOM_KEY.name(), new String[] {"1", "6"});
    params.put(OccurrenceSearchParameter.COUNTRY.name(), new String[] {"DK"});
    params.put("useDnaAssistance", new String[] {"true"});

    ConjunctionPredicate predicate = (ConjunctionPredicate) PredicateFactory.build(params);

    for (Predicate p : predicate.getPredicates()) {
      if (p instanceof EqualsPredicate<?> equals) {
        if (equals.getKey() == OccurrenceSearchParameter.TAXON_KEY) {
          assertEquals(Boolean.TRUE, equals.getUseDnaAssistance());
        } else {
          // it only applies to the taxonomic parameters
          assertEquals(OccurrenceSearchParameter.COUNTRY, equals.getKey());
          assertNull(equals.getUseDnaAssistance());
        }
      } else {
        InPredicate<?> in = (InPredicate<?>) p;
        assertEquals(OccurrenceSearchParameter.KINGDOM_KEY, in.getKey());
        assertEquals(Boolean.TRUE, in.getUseDnaAssistance());
      }
    }
  }

  @Test
  public void testWithoutUseDnaAssistance() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.TAXON_KEY.name(), new String[] {"212"});
    params.put("useDnaAssistance", new String[] {"false"});

    EqualsPredicate<?> predicate = (EqualsPredicate<?>) PredicateFactory.build(params);

    assertNull(predicate.getUseDnaAssistance());
  }

  @Test
  public void testDnaAssistedChecklistKey() {
    UUID dnaChecklistKey = UUID.fromString("7ddf754f-d193-4cc9-b351-99906754a03b");
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.TAXON_KEY.name(), new String[] {"212"});
    params.put(OccurrenceSearchParameter.COUNTRY.name(), new String[] {"DK"});
    params.put("dnaAssistedChecklistKey", new String[] {dnaChecklistKey.toString()});

    ConjunctionPredicate predicate = (ConjunctionPredicate) PredicateFactory.build(params);

    // the dnaAssistedChecklistKey is set in the taxonomic predicates, and is also a filter
    assertEquals(3, predicate.getPredicates().size());
    for (Predicate p : predicate.getPredicates()) {
      EqualsPredicate<?> equals = (EqualsPredicate<?>) p;
      if (equals.getKey() == OccurrenceSearchParameter.TAXON_KEY) {
        assertEquals(dnaChecklistKey, equals.getDnaAssistedChecklistKey());
      } else if (equals.getKey() == OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY) {
        assertEquals(dnaChecklistKey.toString(), equals.getValue());
        assertNull(equals.getDnaAssistedChecklistKey());
      } else {
        // it only applies to the taxonomic parameters
        assertEquals(OccurrenceSearchParameter.COUNTRY, equals.getKey());
        assertNull(equals.getDnaAssistedChecklistKey());
      }
    }
  }

  @Test
  public void testUseDnaAssistanceIgnoresDnaAssistedChecklistKey() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.TAXON_KEY.name(), new String[] {"212"});
    params.put("useDnaAssistance", new String[] {"true"});
    params.put("dnaAssistedChecklistKey", new String[] {"7ddf754f-d193-4cc9-b351-99906754a03b"});

    // the dnaAssistedChecklistKey is ignored with useDnaAssistance, it isn't a filter either
    EqualsPredicate<?> predicate = (EqualsPredicate<?>) PredicateFactory.build(params);
    assertEquals(OccurrenceSearchParameter.TAXON_KEY, predicate.getKey());
  }

  @Test
  public void testTaxonomicRangeIsRejectedWithUseDnaAssistance() {
    // the taxonomic parameters aren't numeric, so a range never becomes a (DNA-unaware)
    // range predicate
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.KINGDOM_KEY.name(), new String[] {"1,6"});
    params.put("useDnaAssistance", new String[] {"true"});

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> PredicateFactory.build(params));
    assertTrue(e.getMessage().startsWith("Ranges are only supported"));
  }

  private static final String COL_KEY = "2d59e5db-57ad-41ff-97d6-11f5fb264527";

  @Test
  public void testChecklistKeyWithoutDna() {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.CHECKLIST_KEY.name(), new String[] {COL_KEY});

    // unchanged, only the checklists of the regular classifications
    EqualsPredicate<?> predicate = (EqualsPredicate<?>) PredicateFactory.build(params);
    assertEquals(OccurrenceSearchParameter.CHECKLIST_KEY, predicate.getKey());
  }

  @Test
  public void testChecklistKeyWithUseDnaAssistance() throws Exception {
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.CHECKLIST_KEY.name(), new String[] {COL_KEY});
    params.put("useDnaAssistance", new String[] {"true"});

    Predicate predicate = PredicateFactory.build(params);

    // a record without a regular match in the checklist, but with a DNA-assisted one, is in it
    assertDnaClassificationAlternative(predicate, UUID.fromString(COL_KEY));
    String query =
        new OccurrenceEsQueryVisitor(OccurrenceEsField.buildFieldMapper(), UUID.fromString(COL_KEY))
            .buildQuery(predicate);
    assertTrue(query.contains("\"checklistKey\""), query);
    assertTrue(query.contains("\"dnaChecklistKey\""), query);
  }

  @Test
  public void testChecklistKeyWithDnaAssistedChecklistKey() {
    UUID dnaChecklistKey = UUID.fromString("7ddf754f-d193-4cc9-b351-99906754a03b");
    Map<String, String[]> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.CHECKLIST_KEY.name(), new String[] {COL_KEY});
    params.put("dnaAssistedChecklistKey", new String[] {dnaChecklistKey.toString()});

    ConjunctionPredicate predicate = (ConjunctionPredicate) PredicateFactory.build(params);

    // in DNA-only mode it's the DNA-assisted match in the dnaAssistedChecklistKey, which is also
    // a filter itself
    assertEquals(2, predicate.getPredicates().size());
    assertDnaClassificationAlternative(
        predicate.getPredicates().stream()
            .filter(DisjunctionPredicate.class::isInstance)
            .findFirst()
            .orElseThrow(),
        dnaChecklistKey);
    assertTrue(
        predicate.getPredicates().stream()
            .anyMatch(
                p ->
                    p instanceof EqualsPredicate<?> equals
                        && equals.getKey() == OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY
                        && equals.getValue().equals(dnaChecklistKey.toString())));
  }

  private static void assertDnaClassificationAlternative(Predicate predicate, UUID dnaChecklistKey) {
    DisjunctionPredicate or = (DisjunctionPredicate) predicate;
    assertEquals(2, or.getPredicates().size());
    Map<Object, String> values =
        or.getPredicates().stream()
            .map(p -> (EqualsPredicate<?>) p)
            .collect(Collectors.toMap(EqualsPredicate::getKey, EqualsPredicate::getValue));
    assertEquals(COL_KEY, values.get(OccurrenceSearchParameter.CHECKLIST_KEY));
    assertEquals(
        dnaChecklistKey.toString(), values.get(OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY));
  }
}
