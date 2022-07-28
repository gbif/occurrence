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

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.elasticsearch.search.SearchHit;

import com.google.common.base.Strings;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public abstract class SearchHitConverter<T> implements Function<SearchHit, T> {

  private static final Pattern NESTED_PATTERN = Pattern.compile("^\\w+(\\.\\w+)+$");
  private static final Predicate<String> IS_NESTED = s -> NESTED_PATTERN.matcher(s).find();

  protected static final TermFactory TERM_FACTORY = TermFactory.instance();

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

  protected final EsFieldMapper esFieldMapper;

  protected Optional<String> getStringValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Function.identity());
  }

  protected Optional<Integer> getIntValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Integer::valueOf);
  }

  protected Optional<Double> getDoubleValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Double::valueOf);
  }

  protected Optional<Date> getDateValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, STRING_TO_DATE);
  }

  protected Optional<Boolean> getBooleanValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Boolean::valueOf);
  }

  public String getValueFieldName(OccurrenceEsField occurrenceEsField) {
    return esFieldMapper.getFieldName(occurrenceEsField, occurrenceEsField.getValueFieldName());
  }

  protected Optional<List<String>> getListValue(SearchHit hit, OccurrenceEsField esField) {
    return getComplexValue(hit, esField,v -> {
      List<String> value = (List<String>) v;
      return value.isEmpty()? null : value;
    });
  }

  protected Optional<String> getListValueAsString(SearchHit hit, OccurrenceEsField esField) {
    return getComplexValue(hit, esField,v -> {
      List<String> value = (List<String>) v;
      return value.isEmpty()? null : String.join("|", value);
    });
  }

  protected Optional<Map<String,Object>> getMapValue(SearchHit hit, OccurrenceEsField esField) {
    return getComplexValue(hit, esField,v -> {
                            Map<String,Object> value = (Map<String,Object>) v;
                            return value.keySet().isEmpty()? null : value;
                           });
  }

  protected Optional<List<Map<String, Object>>> getObjectsListValue(SearchHit hit, OccurrenceEsField esField) {
    return getComplexValue(hit, esField,
                           v -> {
                             List<Map<String, Object>> value = (List<Map<String, Object>>) v;
                             return value.isEmpty()? null : value;
    });
  }

  protected Map<String,Object> getNestedFieldValue(Map<String, Object> fields, String fieldName) {
    // take all paths till the field name
    String[] paths = fieldName.split("\\.");
    for (int i = 0; i < paths.length - 1 && fields.get(paths[i]) != null; i++) {
      // update the fields with the current path
      fields = (Map<String, Object>) fields.get(paths[i]);
    }
    return fields;
  }

  private boolean isNested(String fieldName) {
    return IS_NESTED.test(fieldName);
  }

  protected String getNestedFieldName(String fieldName) {
    String[] paths = fieldName.split("\\.");
    return paths[paths.length - 1];
  }


  protected <T> Optional<T> getComplexValue(SearchHit hit, OccurrenceEsField esField, Function<Object, T> mapper) {
    String fieldName =  getValueFieldName(esField);
    Map<String, Object> fields = hit.getSourceAsMap();
    if (isNested(fieldName)) {
      fields = getNestedFieldValue(fields, fieldName);
      fieldName = getNestedFieldName(fieldName);
    }
    return extractComplexValue(fields, fieldName, mapper);
  }

  protected <T> Optional<T> getValue(SearchHit hit, OccurrenceEsField esField, Function<String, T> mapper) {
    String fieldName =  getValueFieldName(esField);
    Map<String, Object> fields = hit.getSourceAsMap();
    if (isNested(fieldName)) {
      fields = getNestedFieldValue(fields, fieldName);
      fieldName = getNestedFieldName(fieldName);
    }
    return extractStringValue(fields, fieldName, mapper);
  }

  protected <T> Optional<T> extractStringValue(Map<String, Object> fields, String fieldName, Function<String, T> mapper) {
    return extractComplexValue(fields, fieldName, v -> mapper.apply(String.valueOf(v)));
  }

  protected <T> Optional<T> extractComplexValue(Map<String, Object> fields, String fieldName, Function<Object, T> mapper) {
    if (fields == null || fieldName == null || mapper == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(fields.get(fieldName))
      .map(v -> {
        try {
          return mapper.apply(v);
        } catch (Exception ex) {
          log.error("Error extracting field {} with value {}", fieldName, v);
          return null;
        }
      });
  }

  protected Optional<String> extractStringValue(Map<String, Object> fields, String fieldName) {
    return extractStringValue(fields, fieldName, Function.identity());
  }

  /**
   * Re-maps terms to handle Unknown terms.
   * This has to be done because Pipelines preserve Unknown terms and do not add the URI for unknown terms.
   */
  protected static Term mapTerm(String verbatimTerm) {
    Term term  = TERM_FACTORY.findTerm(verbatimTerm);
    if (term instanceof UnknownTerm) {
      return UnknownTerm.build(term.simpleName(), false);
    }
    return term;
  }

  @Override
  public abstract T apply(SearchHit hit);
}
