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

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.*;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.RequestOptions;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class EsQueryUtils {

  private EsQueryUtils() {}

  // defaults
  private static final int DEFAULT_FACET_OFFSET = 0;
  private static final int DEFAULT_FACET_LIMIT = 10;

  // ES fields for queries
  public static final String SIZE = "size";
  public static final String FROM = "from";
  public static final String TO = "to";
  public static final String QUERY = "query";
  public static final String BOOL = "bool";
  public static final String MUST = "must";
  public static final String MATCH = "match";
  public static final String TERM = "term";
  public static final String TERMS = "terms";
  public static final String FILTER = "filter";
  public static final String SHOULD = "should";
  public static final String RANGE = "range";
  public static final String GTE = "gte";
  public static final String LTE = "lte";
  public static final String VALUE = "value";
  public static final String POST_FILTER = "post_filter";
  public static final String SUGGEST = "suggest";

  // Aggs
  public static final String FIELD = "field";
  public static final String AGGREGATIONS = "aggregations";
  public static final String AGGS = "aggs";
  public static final String PRECISION = "precision";
  public static final String GEOHASH_GRID = "geohash_grid";
  public static final String GEO_BOUNDS = "geo_bounds";
  public static final String GEO_BOUNDING_BOX = "geo_bounding_box";

  // geo_shape
  static final String GEO_SHAPE = "geo_shape";
  static final String COORDINATES = "coordinates";
  static final String TYPE = "type";
  static final String SHAPE = "shape";
  static final String RELATION = "relation";
  static final String WITHIN = "within";

  static final String RANGE_SEPARATOR = ",";
  static final String RANGE_WILDCARD = "*";

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern(
          "[yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mmXXX][yyyy-MM-dd'T'HH:mm:ss.SSS XXX][yyyy-MM-dd'T'HH:mm:ss.SSSXXX]"
              + "[yyyy-MM-dd'T'HH:mm:ss.SSSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSS][yyyy-MM-dd'T'HH:mm:ss.SSS]"
              + "[yyyy-MM-dd'T'HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss XXX][yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mm:ss]"
              + "[yyyy-MM-dd'T'HH:mm][yyyy-MM-dd][yyyy-MM][yyyy]");

  static final Function<String, Date> STRING_TO_DATE =
      dateAsString -> {
        if (Strings.isNullOrEmpty(dateAsString)) {
          return null;
        }

        boolean firstYear = false;
        if (dateAsString.startsWith("0000")) {
          firstYear = true;
          dateAsString = dateAsString.replaceFirst("0000", "1970");
        }

        // parse string
        TemporalAccessor temporalAccessor = FORMATTER.parseBest(dateAsString,
                                                                ZonedDateTime::from,
                                                                LocalDateTime::from,
                                                                LocalDate::from,
                                                                YearMonth::from,
                                                                Year::from);
        Date dateParsed = null;
        if (temporalAccessor instanceof ZonedDateTime) {
          dateParsed = Date.from(((ZonedDateTime)temporalAccessor).toInstant());
        } else if (temporalAccessor instanceof LocalDateTime) {
          dateParsed = Date.from(((LocalDateTime)temporalAccessor).toInstant(ZoneOffset.UTC));
        } else if (temporalAccessor instanceof LocalDate) {
          dateParsed = Date.from((((LocalDate)temporalAccessor).atStartOfDay()).toInstant(ZoneOffset.UTC));
        } else if (temporalAccessor instanceof YearMonth) {
          dateParsed = Date.from((((YearMonth)temporalAccessor).atDay(1)).atStartOfDay().toInstant(ZoneOffset.UTC));
        } else if (temporalAccessor instanceof Year) {
          dateParsed = Date.from((((Year)temporalAccessor).atDay(1)).atStartOfDay().toInstant(ZoneOffset.UTC));
        }

        if (dateParsed != null && firstYear) {
          Calendar cal = Calendar.getInstance();
          cal.setTime(dateParsed);
          cal.set(Calendar.YEAR, 1);
          return cal.getTime();
        }

        return dateParsed;
      };

  static final Function<String, LocalDateTime> LOWER_BOUND_RANGE_PARSER =
      lowerBound -> {
        if (Strings.isNullOrEmpty(lowerBound) || RANGE_WILDCARD.equalsIgnoreCase(lowerBound)) {
          return null;
        }

        TemporalAccessor temporalAccessor =
            FORMATTER.parseBest(lowerBound, LocalDate::from, YearMonth::from, Year::from);

        if (temporalAccessor instanceof LocalDate) {
          return ((LocalDate) temporalAccessor).atTime(LocalTime.MIN);
        }

        if (temporalAccessor instanceof Year) {
          return Year.from(temporalAccessor).atMonth(Month.JANUARY).atDay(1).atTime(LocalTime.MIN);
        }

        if (temporalAccessor instanceof YearMonth) {
          return YearMonth.from(temporalAccessor).atDay(1).atTime(LocalTime.MIN);
        }

        return null;
      };

  static final Function<String, LocalDateTime> UPPER_BOUND_RANGE_PARSER =
      upperBound -> {
        if (Strings.isNullOrEmpty(upperBound) || RANGE_WILDCARD.equalsIgnoreCase(upperBound)) {
          return null;
        }

        TemporalAccessor temporalAccessor =
            FORMATTER.parseBest(upperBound, LocalDate::from, YearMonth::from, Year::from);

        if (temporalAccessor instanceof LocalDate) {
          return ((LocalDate) temporalAccessor).atTime(LocalTime.MAX);
        }

        if (temporalAccessor instanceof Year) {
          return Year.from(temporalAccessor)
              .atMonth(Month.DECEMBER)
              .atEndOfMonth()
              .atTime(LocalTime.MAX);
        }

        if (temporalAccessor instanceof YearMonth) {
          return YearMonth.from(temporalAccessor).atEndOfMonth().atTime(LocalTime.MAX);
        }

        return null;
      };

  // functions
  public static final Supplier<RequestOptions> HEADERS =
      () -> {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
        return builder.build();
      };

  static final Map<OccurrenceEsField, Integer> CARDINALITIES =
      ImmutableMap.<OccurrenceEsField, Integer>builder()
          .put(OccurrenceEsField.BASIS_OF_RECORD, BasisOfRecord.values().length)
          .put(OccurrenceEsField.COUNTRY_CODE, Country.values().length)
          .put(OccurrenceEsField.PUBLISHING_COUNTRY, Country.values().length)
          .put(OccurrenceEsField.CONTINENT, Continent.values().length)
          .put(OccurrenceEsField.ESTABLISHMENT_MEANS, EstablishmentMeans.values().length)
          .put(OccurrenceEsField.ISSUE, OccurrenceIssue.values().length)
          .put(OccurrenceEsField.LICENSE, License.values().length)
          .put(OccurrenceEsField.MEDIA_TYPE, MediaType.values().length)
          .put(OccurrenceEsField.TYPE_STATUS, TypeStatus.values().length)
          .put(OccurrenceEsField.KINGDOM_KEY, Kingdom.values().length)
          .put(OccurrenceEsField.MONTH, 12)
          .put(OccurrenceEsField.IUCN_RED_LIST_CATEGORY, ThreatStatus.values().length)
          .build();

  static final List<OccurrenceEsField> DATE_FIELDS =
    ImmutableList.of(
      OccurrenceEsField.EVENT_DATE,
      OccurrenceEsField.DATE_IDENTIFIED,
      OccurrenceEsField.MODIFIED,
      OccurrenceEsField.LAST_INTERPRETED,
      OccurrenceEsField.LAST_CRAWLED,
      OccurrenceEsField.LAST_PARSED);

  static final Map<String, OccurrenceSearchParameter> ES_TO_SEARCH_MAPPING =
      new HashMap<>(EsFieldMapper.SEARCH_TO_ES_MAPPING.size());

  static {
    for (Map.Entry<OccurrenceSearchParameter, OccurrenceEsField> paramField :
        EsFieldMapper.SEARCH_TO_ES_MAPPING.entrySet()) {
      ES_TO_SEARCH_MAPPING.put(paramField.getValue().getSearchFieldName(), paramField.getKey());
    }
  }


  static int extractFacetLimit(OccurrenceSearchRequest request, OccurrenceSearchParameter facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
        .map(Pageable::getLimit)
        .orElse(request.getFacetLimit() != null ? request.getFacetLimit() : DEFAULT_FACET_LIMIT);
  }

  static int extractFacetOffset(OccurrenceSearchRequest request, OccurrenceSearchParameter facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
        .map(v -> (int) v.getOffset())
        .orElse(request.getFacetOffset() != null ? request.getFacetOffset() : DEFAULT_FACET_OFFSET);
  }

}
