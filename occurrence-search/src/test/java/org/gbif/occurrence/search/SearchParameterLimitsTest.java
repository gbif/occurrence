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
package org.gbif.occurrence.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.junit.jupiter.api.Test;

class SearchParameterLimitsTest {

  @Test
  void countPoints() {
    assertEquals(1, SearchParameterLimits.countPoints("POINT (30 10)"));
    assertEquals(5, SearchParameterLimits.countPoints("POLYGON((30 10,40 40, 20 40, 10 20, 30 10))"));
    assertEquals(
        8,
        SearchParameterLimits.countPoints(
            "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 15 5)))"));
    assertEquals(4, SearchParameterLimits.countPoints("POLYGON((-1.5 -2e3, +3 .4, 1 1, -1.5 -2e3))"));
  }

  @Test
  void geometryPointLimit() {
    String small = polygon(SearchParameterLimits.MAX_GEOMETRY_POINTS);
    SearchParameterLimits.validate(OccurrenceSearchParameter.GEOMETRY, List.of(small));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SearchParameterLimits.validate(
                OccurrenceSearchParameter.GEOMETRY,
                List.of(polygon(SearchParameterLimits.MAX_GEOMETRY_POINTS + 1))));

    // the limit is for all the geometries together
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchParameterLimits.validate(OccurrenceSearchParameter.GEOMETRY, List.of(small, small)));
  }

  @Test
  void lookupValueLimit() {
    List<String> names = values(SearchParameterLimits.MAX_LOOKUP_VALUES);
    SearchParameterLimits.validate(OccurrenceSearchParameter.SCIENTIFIC_NAME, names);

    List<String> tooMany = values(SearchParameterLimits.MAX_LOOKUP_VALUES + 1);
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchParameterLimits.validate(OccurrenceSearchParameter.SCIENTIFIC_NAME, tooMany));
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchParameterLimits.validate(OccurrenceSearchParameter.GEOLOGICAL_TIME, tooMany));

    // other parameters aren't limited here
    SearchParameterLimits.validate(OccurrenceSearchParameter.TAXON_KEY, tooMany);
    SearchParameterLimits.validate(OccurrenceSearchParameter.TAXON_KEY, Collections.emptyList());
  }

  private static List<String> values(int count) {
    return IntStream.range(0, count).mapToObj(Integer::toString).collect(Collectors.toList());
  }

  /** A polygon with the given number of points (the last one closes the ring). */
  private static String polygon(int points) {
    String ring =
        IntStream.range(0, points - 1)
            .mapToObj(i -> (i % 90) + " " + (i / 90))
            .collect(Collectors.joining(", "));
    return "POLYGON((" + ring + ", 0 0))";
  }
}
