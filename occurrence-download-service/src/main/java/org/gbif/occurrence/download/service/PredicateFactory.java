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

import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GeoDistancePredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for dealing with the decoding of the request parameters to the
 * query object to pass into the service.
 * This parses the URL params which should be from something like the following
 * into a predicate suitable for launching a download service.
 * It understands multi valued parameters and interprets the range format *,100
 * {@literal TAXON_KEY=12&ELEVATION=1000,2000
 * (ELEVATION >= 1000 AND ELEVATION <= 1000)}
 */
public class PredicateFactory {

  private static final String WILDCARD = "*";

  /**
   * Making constructor private.
   */
  private PredicateFactory() {
    //empty constructor
  }

  /**
   * Builds a full predicate filter from the parameters.
   * In case no filters exist still return a predicate that matches anything.
   *
   * @return always some predicate
   */
  public static Predicate build(Map<String, String[]> params) {
    // predicates for different parameters. If multiple values for the same parameter exist these are in here already
    List<Predicate> groupedByParam = new ArrayList<>();
    for (Map.Entry<String,String[]> p : params.entrySet()) {
      // recognize valid params by enum name, ignore others
      OccurrenceSearchParameter param = toEnumParam(p.getKey());
      boolean matchCase = Optional.ofNullable(params.get("matchCase")).map(vals -> Boolean.parseBoolean(vals[0])).orElse(false);
      String[] values = p.getValue();
      if (param != null && values != null && values.length > 0) {
        // valid parameter
        Predicate predicate = buildParamPredicate(param, matchCase, values);
        if (predicate != null) {
          groupedByParam.add(predicate);
        }
      }
    }

    if (groupedByParam.isEmpty()) {
      // no filter at all
      return null;

    } else if (groupedByParam.size() == 1) {
      return groupedByParam.get(0);

    } else {
      // AND the individual params
      return new ConjunctionPredicate(groupedByParam);
    }
  }

  /**
   * @return the search enum or null if it cant be converted
   */
  private static OccurrenceSearchParameter toEnumParam(String name) {
    try {
      return VocabularyUtils.lookupEnum(name, OccurrenceSearchParameter.class);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static Predicate buildParamPredicate(OccurrenceSearchParameter param, boolean matchCase, String... values) {
    boolean allEquals = true;
    List<Predicate> predicates = new ArrayList<>();
    for (String v : values) {
      Predicate p = parsePredicate(param, v, matchCase);
      allEquals &= p instanceof EqualsPredicate;
      if (p != null) {
        predicates.add(p);
      }
    }

    if (predicates.isEmpty()) {
      return null;

    } else if (predicates.size() == 1) {
      return predicates.get(0);

    } else if (allEquals) {
      return new InPredicate(param, Arrays.asList(values), matchCase);
    } else {
      return new DisjunctionPredicate(predicates);
    }
  }

  private static String toIsoDate(LocalDate d) {
    return d.format(DateTimeFormatter.ISO_DATE);
  }

  /**
   * Converts a value with an optional predicate prefix into a real predicate instance, defaulting to EQUALS.
   */
  private static Predicate parsePredicate(OccurrenceSearchParameter param, String value, boolean matchCase) {
    // geometry filters are special
    if (OccurrenceSearchParameter.GEOMETRY == param) {
      // validate it here, as this constructor only logs invalid strings.
      SearchTypeValidator.validate(OccurrenceSearchParameter.GEOMETRY, value);
      return new WithinPredicate(value);
    }

    // geo_distance filters
    if (OccurrenceSearchParameter.GEO_DISTANCE == param) {
      // validate it here, as this constructor only logs invalid strings.
      SearchTypeValidator.validate(OccurrenceSearchParameter.GEO_DISTANCE, value);
      return new GeoDistancePredicate(DistanceUnit.GeoDistance.parseGeoDistance(value));
    }

    // test for ranges
    if (SearchTypeValidator.isRange(value)) {
      Range<?> range;
      if (Double.class.equals(param.type())) {
        range = SearchTypeValidator.parseDecimalRange(value);
      } else if (Integer.class.equals(param.type())) {
        range = SearchTypeValidator.parseIntegerRange(value);
      } else if (Date.class.equals(param.type())) {
        range = SearchTypeValidator.parseDateRange(value);
        // convert date instances back to strings, but keep the new precision which is now always up to the day!
        range = Range.closed(
          range.hasLowerBound() ? toIsoDate((LocalDate) range.lowerEndpoint()) : null,
          range.hasUpperBound() ? toIsoDate((LocalDate) range.upperEndpoint()) : null
          );
      } else {
        throw new IllegalArgumentException(
          "Ranges are only supported for numeric or date parameter types but received " + param);
      }


      List<Predicate> rangePredicates = new ArrayList<>();
      if (range.hasLowerBound()) {
        rangePredicates.add(new GreaterThanOrEqualsPredicate(param, range.lowerEndpoint().toString()));
      }
      if (range.hasUpperBound()) {
        rangePredicates.add(new LessThanOrEqualsPredicate(param, range.upperEndpoint().toString()));
      }

      if (rangePredicates.size() == 1) {
        return rangePredicates.get(0);
      }
      if (rangePredicates.size() > 1) {
        return new ConjunctionPredicate(rangePredicates);
      }
      return null;

    } else {
      if (WILDCARD.equals(value)) {
        return new IsNotNullPredicate(param);
      } else {
        // defaults to an equals predicate with the original value
        return new EqualsPredicate(param, value, matchCase);
      }
    }
  }
}
