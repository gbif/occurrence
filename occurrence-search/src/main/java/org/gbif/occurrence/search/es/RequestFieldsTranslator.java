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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.common.Strings;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.vocabulary.DurationUnit;
import org.gbif.vocabulary.api.ConceptView;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.vocabulary.model.Tag;

public class RequestFieldsTranslator {

  // TODO: add to this class the dnaSequence conversion too?

  private static final String GEO_TIME_VOCAB = "GeoTime";
  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .registerModule(
              new SimpleModule()
                  .addDeserializer(
                      OccurrenceSearchParameter.class,
                      new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer()))
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @SneakyThrows
  public static Predicate translateVocabs(Predicate predicate, ConceptClient conceptClient) {
    if (conceptClient == null || predicate == null) {
      return predicate;
    }

    ObjectNode node = objectMapper.valueToTree(predicate);
    node.findParents("key")
        .forEach(
            parent -> {
              if (parent
                  .findValue("key")
                  .asText()
                  .equals(OccurrenceSearchParameter.GEOLOGICAL_TIME.name())) {
                String translatedParam =
                    processGeoTimeParam(parent.findValue("value").asText(), conceptClient);
                ((ObjectNode) parent).replace("value", new TextNode(translatedParam));
              } else if (parent
                  .findValue("key")
                  .asText()
                  .equals(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION.name())) {
                String translatedParam =
                    processHumboldtEventDurationParam(parent.findValue("value").asText());
                ((ObjectNode) parent).replace("value", new TextNode(translatedParam));
                ((ObjectNode) parent)
                    .replace(
                        "key",
                        new TextNode(
                            OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES
                                .name()));
              }
            });

    return objectMapper.treeToValue(node, Predicate.class);
  }

  public static void translateVocabs(
      OccurrenceSearchRequest searchRequest, ConceptClient conceptClient) {
    translateVocabs(searchRequest.getParameters(), conceptClient);
  }

  public static void translateVocabs(
      Map<OccurrenceSearchParameter, Set<String>> params, ConceptClient conceptClient) {
    if (conceptClient == null) {
      return;
    }

    if (params.containsKey(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION)) {
      String durationParam =
          params.get(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION).iterator().next();
      String translatedParam = processHumboldtEventDurationParam(durationParam);
      params.put(
          OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES,
          Set.of(translatedParam));
      params.remove(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION);
    }

    if (params.containsKey(OccurrenceSearchParameter.GEOLOGICAL_TIME)) {
      String geoTimeParam = params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next();
      String translatedParam = processGeoTimeParam(geoTimeParam, conceptClient);
      params.replace(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of(translatedParam));
    }
  }

  private static String processHumboldtEventDurationParam(String durationParam) {
    if (Strings.isNullOrEmpty(durationParam)) {
      return durationParam;
    }

    if (durationParam.contains(EsQueryUtils.RANGE_SEPARATOR)) {
      String[] range = durationParam.split(",");
      String lowerBound = range[0];
      String higherBound = range[1];

      String translatedRange = "";

      if (lowerBound.equals(EsQueryUtils.RANGE_WILDCARD)) {
        translatedRange = "*,";
      } else {
        DurationUnit.Duration lowerDuration =
            DurationUnit.Duration.parse(lowerBound)
                .orElseThrow(
                    () -> new IllegalArgumentException("Invalid humboldt event lower duration"));
        translatedRange += lowerDuration.toMinutes() + ",";
      }

      if (higherBound.equals(EsQueryUtils.RANGE_WILDCARD)) {
        translatedRange += "*";
      } else {
        DurationUnit.Duration higherDuration =
            DurationUnit.Duration.parse(higherBound)
                .orElseThrow(
                    () -> new IllegalArgumentException("Invalid humboldt event higher duration"));
        translatedRange += higherDuration.toMinutes();
      }

      return translatedRange;
    } else {
      DurationUnit.Duration duration =
          DurationUnit.Duration.parse(durationParam)
              .orElseThrow(() -> new IllegalArgumentException("Invalid humboldt event duration"));
      return String.valueOf(duration.toMinutes());
    }
  }

  private static String processGeoTimeParam(String geoTimeParam, ConceptClient conceptClient) {
    if (Strings.isNullOrEmpty(geoTimeParam)) {
      return geoTimeParam;
    }

    GeotimeAges geotimeAges = null;
    if (geoTimeParam.contains(EsQueryUtils.RANGE_SEPARATOR)) {
      String[] range = geoTimeParam.split(",");
      String lowerBound = range[0];
      String higherBound = range[1];

      GeotimeAges geotimeAgesLower = getGeotimeAges(conceptClient, lowerBound);
      GeotimeAges geotimeAgesHigher = getGeotimeAges(conceptClient, higherBound);

      // check if a concept is contained by the other
      if (isRangeContained(geotimeAgesLower, geotimeAgesHigher)
          || isRangeContained(geotimeAgesHigher, geotimeAgesLower)) {
        throw new IllegalArgumentException("One of the concepts is contained by the other");
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

      return end + EsQueryUtils.RANGE_SEPARATOR + start;

    } else {
      GeotimeAges geotimeAgesSingleParam = getGeotimeAges(conceptClient, geoTimeParam);
      if (geotimeAgesSingleParam.startAge != null) {
        return geotimeAgesSingleParam.startAge;
      }
    }

    return geoTimeParam;
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
