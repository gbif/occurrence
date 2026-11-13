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

import org.gbif.api.model.common.search.SearchParameter;

import java.util.Collection;
import java.util.Set;

/**
 * Limits on search parameter values, checked before the values are parsed or used to call other
 * services.
 */
public final class SearchParameterLimits {

  /** Maximum number of points in all the geometries of a search request. */
  public static final int MAX_GEOMETRY_POINTS = 2000;

  /** Maximum number of values for a parameter that needs a call to another service per value. */
  public static final int MAX_LOOKUP_VALUES = 100;

  // Each value is matched against a checklist or a vocabulary
  private static final Set<String> LOOKUP_PARAMETERS = Set.of("SCIENTIFIC_NAME", "GEOLOGICAL_TIME");

  private SearchParameterLimits() {}

  /**
   * Checks all the values of a parameter.
   *
   * @throws IllegalArgumentException if a limit is exceeded
   */
  public static void validate(SearchParameter parameter, Collection<String> values) {
    String name = parameter.name();
    if (LOOKUP_PARAMETERS.contains(name) && values.size() > MAX_LOOKUP_VALUES) {
      throw new IllegalArgumentException(
          "Parameter " + name + " may have at most " + MAX_LOOKUP_VALUES + " values");
    }

    if ("GEOMETRY".equals(name)) {
      long points = values.stream().mapToLong(SearchParameterLimits::countPoints).sum();
      if (points > MAX_GEOMETRY_POINTS) {
        throw new IllegalArgumentException(
            "The geometry (or geometries) contains "
                + points
                + " points. The maximum is "
                + MAX_GEOMETRY_POINTS
                + ".");
      }
    }
  }

  /**
   * Counts the points in a WKT geometry without parsing it: each point is separated from the next by
   * a comma, or is the only point in its parentheses.
   */
  static long countPoints(String wkt) {
    long points = 0;
    boolean inPoint = false;
    for (int i = 0; i < wkt.length(); i++) {
      char c = wkt.charAt(i);
      if (c == ',' || c == '(' || c == ')') {
        inPoint = false;
      } else if (!inPoint && (Character.isDigit(c) || c == '-' || c == '+' || c == '.')) {
        inPoint = true;
        points++;
      }
    }
    return points;
  }
}
