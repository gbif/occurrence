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
package org.gbif.occurrence.search.es;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.junit.jupiter.api.Test;

public class RequestFieldsTranslatorTest {

  @Test
  public void geoTimeTranslationTest() {
    Map<OccurrenceSearchParameter, Set<String>> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("mesozoic,cenozoic"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "0.0,251.902", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("cenozoic,mesozoic"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "0.0,251.902", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("neogene,*"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "*,23.03", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("*,neogene"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals("2.58,*", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("burdigalian,pliocene"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "2.58,20.44", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("neogene,cenozoic"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock()));

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("cenozoic,neogene"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock()));

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("miocene"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals("23.03", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("neogene,foo"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock()));
  }

  @Test
  public void predicatesTest() {
    EqualsPredicate<OccurrenceSearchParameter> equalsPredicate =
        new EqualsPredicate<>(OccurrenceSearchParameter.GEOLOGICAL_TIME, "neogene", false);
    Predicate translatedPredicate =
        RequestFieldsTranslator.translateVocabs(equalsPredicate, new ConceptClientMock());
    assertTrue(translatedPredicate instanceof EqualsPredicate);
    EqualsPredicate<OccurrenceSearchParameter> tep =
        (EqualsPredicate<OccurrenceSearchParameter>) translatedPredicate;
    assertEquals(OccurrenceSearchParameter.GEOLOGICAL_TIME, tep.getKey());
    assertEquals("23.03", tep.getValue());

    EqualsPredicate<OccurrenceSearchParameter> rangeEqualsPredicate =
        new EqualsPredicate<>(
            OccurrenceSearchParameter.GEOLOGICAL_TIME, "mesozoic,cenozoic", false);
    Predicate rangeTranslatedPredicate =
        RequestFieldsTranslator.translateVocabs(rangeEqualsPredicate, new ConceptClientMock());
    assertTrue(rangeTranslatedPredicate instanceof EqualsPredicate);
    EqualsPredicate<OccurrenceSearchParameter> trp =
        (EqualsPredicate<OccurrenceSearchParameter>) rangeTranslatedPredicate;
    assertEquals(OccurrenceSearchParameter.GEOLOGICAL_TIME, trp.getKey());
    assertEquals("0.0,251.902", trp.getValue());

    DisjunctionPredicate disjunctionPredicate =
        new DisjunctionPredicate(Arrays.asList(equalsPredicate, rangeEqualsPredicate));
    Predicate translatedDisjunctionPredicate =
        RequestFieldsTranslator.translateVocabs(disjunctionPredicate, new ConceptClientMock());
    assertTrue(translatedDisjunctionPredicate instanceof DisjunctionPredicate);
    DisjunctionPredicate tdp = (DisjunctionPredicate) translatedDisjunctionPredicate;
    assertEquals(2, tdp.getPredicates().size());
    tdp.getPredicates()
        .forEach(
            p -> {
              assertTrue(p instanceof EqualsPredicate);
              EqualsPredicate<OccurrenceSearchParameter> ep =
                  (EqualsPredicate<OccurrenceSearchParameter>) p;
              assertEquals(OccurrenceSearchParameter.GEOLOGICAL_TIME, ep.getKey());
              assertNotEquals(ep.getValue(), equalsPredicate.getValue());
              assertNotEquals(ep.getValue(), rangeEqualsPredicate.getValue());
            });
  }

  @Test
  public void humboldtEventDurationTranslationTest() {
    Map<OccurrenceSearchParameter, Set<String>> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION, Set.of("1h"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "60.0",
        params
            .get(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES)
            .iterator()
            .next());

    params.put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION, Set.of("1h,2h"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "60.0,120.0",
        params
            .get(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES)
            .iterator()
            .next());

    params.put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION, Set.of("*,2h"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "*,120.0",
        params
            .get(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES)
            .iterator()
            .next());

    params.put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION, Set.of("1h,*"));
    RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "60.0,*",
        params
            .get(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES)
            .iterator()
            .next());

    params.put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION, Set.of("1foo"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock()));

    params.put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION, Set.of("1*,2h"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RequestFieldsTranslator.translateVocabs(params, new ConceptClientMock()));
  }
}
