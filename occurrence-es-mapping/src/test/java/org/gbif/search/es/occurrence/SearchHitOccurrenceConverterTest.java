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
package org.gbif.search.es.occurrence;

import org.gbif.api.model.common.Classification;
import org.gbif.api.model.occurrence.Occurrence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch.core.search.Hit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SearchHitOccurrenceConverterTest {

  private static final String COL_KEY = "7ddf754f-d193-4cc9-b351-99906754a03b";
  private static final String ZA_KEY = "f5dc22ca-0bb1-4692-8a97-cc7ac54d7ed9";

  private final SearchHitOccurrenceConverter converter =
      new SearchHitOccurrenceConverter(OccurrenceEsField.buildFieldMapper(), false);

  /** Without a match in a checklist, only its issues are indexed. */
  @Test
  public void noMatchClassificationTest() {
    Map<String, Object> matched = new HashMap<>();
    matched.put("usage", Map.of("key", "10", "name", "Puma concolor", "rank", "SPECIES"));
    matched.put("classification", Map.of("SPECIES", "Puma concolor"));
    matched.put("classificationKeys", Map.of("SPECIES", "10"));
    matched.put("classificationDepth", Map.of("0", "10"));
    matched.put("issues", List.of());
    Map<String, Object> noMatch = Map.of("issues", List.of("TAXON_MATCH_NONE"));

    Map<String, Object> source = new HashMap<>();
    source.put("classifications", Map.of(COL_KEY, matched, ZA_KEY, noMatch));
    source.put("dnaClassifications", Map.of("classifications", Map.of(COL_KEY, noMatch)));

    Occurrence occurrence =
        converter.apply(Hit.of(h -> h.index("occurrence").id("1").source(source)));

    assertEquals("10", occurrence.getClassifications().get(COL_KEY).getUsage().getKey());
    assertNoMatch(occurrence.getClassifications().get(ZA_KEY));
    assertNoMatch(occurrence.getDnaClassifications().getClassifications().get(COL_KEY));
  }

  private static void assertNoMatch(Classification classification) {
    assertNull(classification.getUsage());
    assertTrue(classification.getClassification().isEmpty());
    assertEquals(List.of("TAXON_MATCH_NONE"), classification.getIssues());
  }
}
