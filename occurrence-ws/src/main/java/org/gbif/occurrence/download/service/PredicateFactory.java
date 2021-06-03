package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GeoDistancePredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
   * @param name
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
    List<Predicate> predicates = new ArrayList<>();
    for (String v : values) {
      Predicate p = parsePredicate(param, v, matchCase);
      if (p != null) {
        predicates.add(p);
      }
    }

    if (predicates.isEmpty()) {
      return null;

    } else if (predicates.size() == 1) {
      return predicates.get(0);

    } else {
      // OR the individual params
      return new DisjunctionPredicate(predicates);
    }
  }

  private static String toIsoDate(Date d) {
    return new SimpleDateFormat("yyyy-MM-dd").format(d);
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
          range.hasLowerBound() ? toIsoDate((Date) range.lowerEndpoint()) : null,
          range.hasUpperBound() ? toIsoDate((Date) range.upperEndpoint()) : null
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
