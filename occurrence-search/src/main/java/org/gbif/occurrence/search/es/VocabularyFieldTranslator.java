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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.vocabulary.api.ConceptView;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.vocabulary.model.Tag;

import java.util.Map;
import java.util.Set;

public class VocabularyFieldTranslator {

  private static final String GEO_TIME_VOCAB = "GeoTime";

  public static void translateVocabs(
      OccurrenceSearchRequest searchRequest, ConceptClient conceptClient) {
    translateVocabs(searchRequest.getParameters(), conceptClient);
  }

  public static void translateVocabs(
      Map<OccurrenceSearchParameter, Set<String>> params, ConceptClient conceptClient) {
    if (conceptClient == null) {
      return;
    }

    if (params.containsKey(OccurrenceSearchParameter.GEOLOGICAL_TIME)) {
      String geoTimeParam = params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next();

      // get the start and end age from the concepts tag
      ConceptView conceptView =
          conceptClient.getFromLatestRelease(GEO_TIME_VOCAB, geoTimeParam, false, false);

      if (conceptView == null) {
        throw new IllegalArgumentException(
            "Concept " + geoTimeParam + " not found in the " + GEO_TIME_VOCAB + " vocabulary");
      }

      String startAge = null;
      for (Tag t : conceptView.getConcept().getTags()) {
        if (t.getName().toLowerCase().startsWith("startage:")) {
          startAge = t.getName().split(":")[1].trim();
        }
      }

      if (startAge != null) {
        params.replace(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of(startAge));
      }
    }
  }
}
