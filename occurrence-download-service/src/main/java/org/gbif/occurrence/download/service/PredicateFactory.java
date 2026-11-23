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
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GeoDistancePredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Utility for dealing with the decoding of the request parameters to the
 * query object to pass into the service.
 * This parses the URL params which should be from something like the following
 * into a predicate suitable for launching a download service.
 * It understands multivalued parameters and interprets the range format *,100
 * {@literal TAXON_KEY=12&ELEVATION=1000,2000
 * (ELEVATION >= 1000 AND ELEVATION <= 1000)}
 */
public class PredicateFactory {

  private static final String WILDCARD = "*";

  private static final String USE_DNA_ASSISTANCE_PARAM = "useDnaAssistance";

  // used to know which parameters are taxonomic and therefore support the DNA assistance
  private static final OccurrenceEsFieldMapper ES_FIELD_MAPPER = OccurrenceEsField.buildFieldMapper();

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
    UUID requestDnaAssistedChecklistKey = getDnaAssistedChecklistKey(params);
    boolean requestUseDnaAssistance = Optional.ofNullable(params.get(USE_DNA_ASSISTANCE_PARAM))
      .map(vals -> Boolean.parseBoolean(vals[0].trim())).orElse(false);
    UUID dnaChecklistKey =
      getDnaChecklistKey(params, requestUseDnaAssistance, requestDnaAssistedChecklistKey);
    for (Map.Entry<String,String[]> p : params.entrySet()) {
      // recognize valid params by enum name, ignore others
      toEnumParam(p.getKey()).ifPresent(param -> {
        // the dnaAssistedChecklistKey is ignored with useDnaAssistance
        if (param == OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY && requestUseDnaAssistance) {
          return;
        }
        boolean matchCase = Optional.ofNullable(params.get("matchCase"))
          .map(vals -> Boolean.parseBoolean(vals[0])).orElse(false);
        // the DNA assistance only applies to the taxonomic parameters
        boolean taxonomic = ES_FIELD_MAPPER.isTaxonomic(param);
        Boolean useDnaAssistance = taxonomic && requestUseDnaAssistance ? Boolean.TRUE : null;
        UUID dnaAssistedChecklistKey = taxonomic ? requestDnaAssistedChecklistKey : null;
        String[] values = p.getValue();
        if (values != null && values.length > 0) {
          // valid parameter
          Predicate predicate =
            buildParamPredicate(param, matchCase, useDnaAssistance, dnaAssistedChecklistKey, values);
          if (predicate != null
              && param == OccurrenceSearchParameter.CHECKLIST_KEY
              && dnaChecklistKey != null) {
            predicate = orDnaClassification(predicate, dnaChecklistKey);
          }
          if (predicate != null) {
            groupedByParam.add(predicate);
          }
        }
      });
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
   * The checklist whose DNA-assisted classification is searched: the checklistKey with
   * useDnaAssistance, otherwise the dnaAssistedChecklistKey. Null without DNA.
   */
  private static UUID getDnaChecklistKey(
      Map<String, String[]> params, boolean useDnaAssistance, UUID dnaAssistedChecklistKey) {
    if (!useDnaAssistance) {
      return dnaAssistedChecklistKey;
    }
    return params.entrySet().stream()
        .filter(e -> toEnumParam(e.getKey())
            .filter(OccurrenceSearchParameter.CHECKLIST_KEY::equals)
            .isPresent())
        .map(Map.Entry::getValue)
        .filter(values -> values != null && values.length > 0)
        .map(values -> UUID.fromString(values[0].trim()))
        .findFirst()
        .orElse(null);
  }

  /**
   * With DNA, a record is also in a checklist when it has a DNA-assisted match in it, as the
   * checklistKey field only has the checklists of the regular classifications.
   */
  private static Predicate orDnaClassification(Predicate checklistKeyPredicate, UUID dnaChecklistKey) {
    return new DisjunctionPredicate(
        List.of(
            checklistKeyPredicate,
            new EqualsPredicate<>(
                OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY, dnaChecklistKey.toString(), false)));
  }

  /**
   * @return the dnaAssistedChecklistKey of the request, or null if it isn't present
   */
  private static UUID getDnaAssistedChecklistKey(Map<String, String[]> params) {
    return params.entrySet().stream()
      .filter(e -> toEnumParam(e.getKey())
        .filter(OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY::equals)
        .isPresent())
      .map(Map.Entry::getValue)
      .filter(values -> values != null && values.length > 0)
      .map(values -> UUID.fromString(values[0].trim()))
      .findFirst()
      .orElse(null);
  }

  /**
   * @return the search enum or empty if it cant be converted
   */
  private static Optional<OccurrenceSearchParameter> toEnumParam(String name) {
    try {
      return OccurrenceSearchParameter.lookup(name);
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  private static Predicate buildParamPredicate(
      OccurrenceSearchParameter param,
      boolean matchCase,
      Boolean useDnaAssistance,
      UUID dnaAssistedChecklistKey,
      String... values) {
    boolean allEquals = true;
    List<Predicate> predicates = new ArrayList<>();
    for (String v : values) {
      Predicate p = parsePredicate(param, v, matchCase, useDnaAssistance, dnaAssistedChecklistKey);
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
      return new InPredicate(
        param, Arrays.asList(values), matchCase, null, useDnaAssistance, dnaAssistedChecklistKey);
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
  private static Predicate parsePredicate(
      OccurrenceSearchParameter param,
      String value,
      boolean matchCase,
      Boolean useDnaAssistance,
      UUID dnaAssistedChecklistKey) {
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
    if (SearchTypeValidator.isNumericRange(value) && (Double.class.equals(param.type()) || Integer.class.equals(param.type()))) {
      Range<?> range = null;
      if (Double.class.equals(param.type())) {
        range = SearchTypeValidator.parseDecimalRange(value);
      } else if (Integer.class.equals(param.type())) {
        range = SearchTypeValidator.parseIntegerRange(value);
      }

      return rangePredicate(param, range);

    } else if (SearchTypeValidator.isDateRange(value) && (Date.class.equals(param.type()) || IsoDateInterval.class.equals(param.type()))) {
      Range<?> range;
      range = SearchTypeValidator.parseDateRange(value);
      // convert date instances back to strings, but keep the new precision which is now always up to the day!
      range = Range.closed(
        range.hasLowerBound() ? toIsoDate((LocalDate) range.lowerEndpoint()) : null,
        range.hasUpperBound() ? toIsoDate((LocalDate) range.upperEndpoint()) : null
      );

      return rangePredicate(param, range);

    } else if (SearchTypeValidator.isNumericRange(value)) {
      throw new IllegalArgumentException(
        "Ranges are only supported for numeric or date parameter types but received " + param);
    } else {
      if (WILDCARD.equals(value)) {
        return new IsNotNullPredicate(param, null, useDnaAssistance, dnaAssistedChecklistKey);
      } else {
        // defaults to an equals predicate with the original value
        return new EqualsPredicate(
          param, value, matchCase, null, useDnaAssistance, dnaAssistedChecklistKey);
      }
    }
  }

  private static Predicate rangePredicate(OccurrenceSearchParameter param, Range<?> range) {
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
  }
}
