package org.gbif.occurrence.search.es;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
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

      GeotimeAges geotimeAges = null;
      if (geoTimeParam.contains(EsQueryUtils.RANGE_SEPARATOR)) {
        String[] range = geoTimeParam.split(",");
        String lowerBound = range[0];
        String higherBound = range[1];

        GeotimeAges geotimeAgesLower = getGeotimeAges(conceptClient, lowerBound);
        GeotimeAges geotimeAgesHigher = getGeotimeAges(conceptClient, higherBound);

        // check if a concept is contained by the other
        if (isRangeContained(geotimeAgesLower, geotimeAgesHigher)) {
          throw new IllegalArgumentException(higherBound + " contains " + lowerBound);
        }

        if (isRangeContained(geotimeAgesHigher, geotimeAgesLower)) {
          throw new IllegalArgumentException(lowerBound + " contains " + higherBound);
        }

        // find lowest and earliest ages
        String start = null;
        String end = null;

        if (lowerBound.equals(EsQueryUtils.RANGE_WILDCARD)) {
          start = EsQueryUtils.RANGE_WILDCARD;
          end = geotimeAgesHigher.endAge;
        } else if (higherBound.equals(EsQueryUtils.RANGE_WILDCARD)) {
          end = EsQueryUtils.RANGE_WILDCARD;
          start = geotimeAgesLower.startAge;
        } else {
          start =
              String.valueOf(
                  Math.max(
                      Optional.ofNullable(geotimeAgesLower.getStartAgeAsFloat()).orElse(0f),
                      Optional.ofNullable(geotimeAgesHigher.getStartAgeAsFloat()).orElse(0f)));

          end =
              String.valueOf(
                  Math.min(
                      Optional.ofNullable(geotimeAgesLower.getEndAgeAsFloat())
                          .orElse(Float.MAX_VALUE),
                      Optional.ofNullable(geotimeAgesHigher.getEndAgeAsFloat())
                          .orElse(Float.MAX_VALUE)));
        }

        params.replace(
            OccurrenceSearchParameter.GEOLOGICAL_TIME,
            Set.of(end + EsQueryUtils.RANGE_SEPARATOR + start));

      } else {
        GeotimeAges geotimeAgesSingleParam = getGeotimeAges(conceptClient, geoTimeParam);
        if (geotimeAgesSingleParam.startAge != null) {
          params.replace(
              OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of(geotimeAgesSingleParam.startAge));
        }
      }
    }
  }

  private static boolean isRangeContained(GeotimeAges ages, GeotimeAges agesContainer) {
    return ages.getStartAgeAsFloat() != null
        && ages.getEndAgeAsFloat() != null
        && agesContainer.getStartAgeAsFloat() != null
        && agesContainer.getEndAgeAsFloat() != null
        && ages.getStartAgeAsFloat() <= agesContainer.getStartAgeAsFloat()
        && ages.getEndAgeAsFloat() >= agesContainer.getEndAgeAsFloat();
  }

  private static GeotimeAges getGeotimeAges(ConceptClient conceptClient, String geoTimeParam) {
    if (geoTimeParam.equals(EsQueryUtils.RANGE_WILDCARD)) {
      return new GeotimeAges(EsQueryUtils.RANGE_WILDCARD, EsQueryUtils.RANGE_WILDCARD);
    }

    // get the start and end age from the concepts tag
    ConceptView conceptView =
        conceptClient.getFromLatestRelease(GEO_TIME_VOCAB, geoTimeParam, false, false);

    if (conceptView == null) {
      throw new IllegalArgumentException(
          "Concept " + geoTimeParam + " not found in the " + GEO_TIME_VOCAB + " vocabulary");
    }

    GeotimeAges geotimeAges = new GeotimeAges();
    for (Tag t : conceptView.getConcept().getTags()) {
      if (t.getName().toLowerCase().startsWith("startage:")) {
        geotimeAges.startAge = t.getName().split(":")[1].trim();
      }
      if (t.getName().toLowerCase().startsWith("endage:")) {
        geotimeAges.endAge = t.getName().split(":")[1].trim();
      }
    }

    return geotimeAges;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  private static class GeotimeAges {
    String startAge;
    String endAge;

    Float getStartAgeAsFloat() {
      return startAge != null && !startAge.equals(EsQueryUtils.RANGE_WILDCARD)
          ? Float.parseFloat(startAge)
          : null;
    }

    Float getEndAgeAsFloat() {
      return endAge != null && !endAge.equals(EsQueryUtils.RANGE_WILDCARD)
          ? Float.parseFloat(endAge)
          : null;
    }
  }
}
