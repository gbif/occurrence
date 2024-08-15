package org.gbif.occurrence.search.es;

import java.util.Map;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.vocabulary.api.ConceptView;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.vocabulary.model.Tag;

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
