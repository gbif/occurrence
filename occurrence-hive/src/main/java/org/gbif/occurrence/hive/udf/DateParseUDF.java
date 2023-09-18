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
package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.occurrence.processor.interpreting.TemporalInterpreter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parses year, month and day only.
 *
 * Usage example:
 *
 * SELECT
 *   gbifid, d.year, d.month, d.day, from_unixtime(floor(d.epoch_from/1000)), from_unixtime(floor(d.epoch_to/1000)),
 *   v_eventdate, v_year, v_month, v_day, v_startdayofyear, v_enddayofyear
 * FROM
 *   (SELECT
 *      gbifid, v_eventdate, v_year, v_month, v_day, v_startdayofyear, v_enddayofyear,
 *      parseDate(v_year, v_month, v_day, v_eventdate, v_startdayofyear, v_enddayofyear) d
 *    FROM prod_h.occurrence
 *    WHERE v_startdayofyear IS NOT NULL
 *      AND v_eventdate IS NULL
 *    LIMIT 10000
 *   ) r
 *  LIMIT 100000;
 */
@Description(
  name = "parseDate",
  value = "_FUNC_(year, month, day, event_date, start_day_of_year, end_day_of_year)")
public class DateParseUDF extends GenericUDF {

  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
    assert arguments.length == 6;

    String year = getArgument(0, arguments);
    String month = getArgument(1, arguments);
    String day = getArgument(2, arguments);
    String event_date = getArgument(3, arguments);
    String start_day_of_year = getArgument(4, arguments);
    String end_day_of_year = getArgument(5, arguments);
    List<Object> result = new ArrayList<Object>(5);

    try {
      OccurrenceParseResult<IsoDateInterval> parsed =
        TemporalInterpreter.interpretRecordedDate(year, month, day, event_date, start_day_of_year, end_day_of_year);
      if (parsed.isSuccessful()) {
        IsoDateInterval dateRange = parsed.getPayload();
        if (dateRange.getTo() != null) {
          if (dateRange.getFrom().isSupported(ChronoField.YEAR) && dateRange.getFrom().get(ChronoField.YEAR) == dateRange.getTo().get(ChronoField.YEAR)) {
            result.add(dateRange.getFrom().get(ChronoField.YEAR));
            if (dateRange.getFrom().isSupported(ChronoField.MONTH_OF_YEAR) && dateRange.getFrom().get(ChronoField.MONTH_OF_YEAR) == dateRange.getTo().get(ChronoField.MONTH_OF_YEAR)) {
              result.add(dateRange.getFrom().get(ChronoField.MONTH_OF_YEAR));
              if (dateRange.getFrom().isSupported(ChronoField.DAY_OF_MONTH) && dateRange.getFrom().get(ChronoField.DAY_OF_MONTH) == dateRange.getTo().get(ChronoField.DAY_OF_MONTH)) {
                result.add(dateRange.getFrom().get(ChronoField.DAY_OF_MONTH));
              } else {
                result.add(null);
              }
            } else {
              result.add(null);
              result.add(null);
            }
          } else {
            result.add(null);
            result.add(null);
            result.add(null);
          }
        } else {
          result.add(null);
          result.add(null);
          result.add(null);
        }
        if (dateRange.getFrom() != null) {
          result.add(TemporalAccessorUtils.toEarliestLocalDateTime(dateRange.getFrom(), true).toEpochSecond(ZoneOffset.UTC));
        } else {
          result.add(null);
        }
        if (dateRange.getTo() != null) {
          result.add(TemporalAccessorUtils.toLatestLocalDateTime(dateRange.getTo(), true).toEpochSecond(ZoneOffset.UTC));
        } else {
          result.add(null);
        }
      } else {
        result.add(null);
        result.add(null);
        result.add(null);
        result.add(null);
        result.add(null);
      }
    } catch (Exception e) {
      // not much to do - indicates bad data
      System.err.println(e.getMessage());
      e.printStackTrace();
    }

    return result;
  }

  public static Long getEarliest(Temporal temporalAccessor) {
    if (temporalAccessor == null) {
      return null;
    } else if (temporalAccessor instanceof ZonedDateTime) {
      return ((ZonedDateTime) temporalAccessor).toInstant().toEpochMilli();
    } else if (temporalAccessor instanceof LocalDateTime) {
      return ((LocalDateTime) temporalAccessor).toInstant(ZoneOffset.UTC).toEpochMilli();
    } else if (temporalAccessor instanceof LocalDate) {
      return ((LocalDate) temporalAccessor).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    } else if (temporalAccessor instanceof YearMonth) {
      return ((YearMonth) temporalAccessor).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    } else if (temporalAccessor instanceof Year) {
      return ((Year) temporalAccessor).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    } else {
      return null;
    }
  }

  public static Long getLatest(Temporal temporalAccessor) {
    if (temporalAccessor == null) {
      return null;
    } else if (temporalAccessor instanceof ZonedDateTime) {
      return ((ZonedDateTime) temporalAccessor).toInstant().toEpochMilli();
    } else if (temporalAccessor instanceof LocalDateTime) {
      return ((LocalDateTime) temporalAccessor).toInstant(ZoneOffset.UTC).toEpochMilli();
    } else if (temporalAccessor instanceof LocalDate) {
      return ((LocalDate) temporalAccessor).atTime(23, 59, 59,999_999_999).toInstant(ZoneOffset.UTC).toEpochMilli();
    } else if (temporalAccessor instanceof YearMonth) {
      return ((YearMonth) temporalAccessor).atEndOfMonth().atTime(23, 59, 59,999_999_999).toInstant(ZoneOffset.UTC).toEpochMilli();
    } else if (temporalAccessor instanceof Year) {
      return ((Year) temporalAccessor).atMonth(12).atEndOfMonth().atTime(23, 59, 59,999_999_999).toInstant(ZoneOffset.UTC).toEpochMilli();
    } else {
      return null;
    }
  }

  private String getArgument(int index, GenericUDF.DeferredObject[] arguments) throws HiveException {
    DeferredObject deferredObject = arguments[index];
    if (deferredObject == null) {
      return null;
    }

    if (deferredObject.get() == null) {
      return null;
    }

    return deferredObject.get().toString();
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == 6;
    return "parseDate(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ", " + strings[3] + ", " + strings[4] + ", " + strings[5] + ')';
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 6) {
      throw new UDFArgumentException("parseDate takes six arguments");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters
        .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList("year", "month", "day", "epoch_from", "epoch_to"), Arrays
        .<ObjectInspector>asList(
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector));
  }

}
