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
package org.gbif.occurrence.trino.udf;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.occurrence.trino.processor.interpreters.TemporalInterpreter;

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.spi.type.BigintType.*;

/**
 * Parses year, month and day only.
 *
 * <p>Usage example:
 *
 * <p>SELECT gbifid, d.year, d.month, d.day, from_unixtime(floor(d.epoch_from/1000)),
 * from_unixtime(floor(d.epoch_to/1000)), v_eventdate, v_year, v_month, v_day, v_startdayofyear,
 * v_enddayofyear FROM (SELECT gbifid, v_eventdate, v_year, v_month, v_day, v_startdayofyear,
 * v_enddayofyear, parseDate(v_year, v_month, v_day, v_eventdate, v_startdayofyear, v_enddayofyear)
 * d FROM prod_h.occurrence WHERE v_startdayofyear IS NOT NULL AND v_eventdate IS NULL LIMIT 10000 )
 * r LIMIT 100000;
 */
public class DateParseUDF {

  @ScalarFunction(value = "parseDate", deterministic = true)
  @Description(
      "Parses the fields of the date. The order of the parameters is the following: parseDate(year, month, day, )."
          + " All of them are nullable. Returns a row(year bigint, month bigint, day bigint, epoch_from bigint, epoch_to bigint)")
  @SqlType("row(year bigint, month bigint, day bigint, epoch_from bigint, epoch_to bigint)")
  public Block parseDate(
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice year,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice month,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice day) {
    return parseDate(year, month, day, null, null, null);
  }

  @ScalarFunction(value = "parseDate", deterministic = true)
  @Description(
      "Parses the fields of the date. The order of the parameters is the following: parseDate(year, month, day, "
          + "event_date). All of them are nullable. "
          + "Returns a row(year bigint, month bigint, day bigint, epoch_from bigint, epoch_to bigint)")
  @SqlType("row(year bigint, month bigint, day bigint, epoch_from bigint, epoch_to bigint)")
  public Block parseDate(
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice year,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice month,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice day,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice eventDate) {
    return parseDate(year, month, day, eventDate, null, null);
  }

  @ScalarFunction(value = "parseDate", deterministic = true)
  @Description(
      "Parses the fields of the date. The order of the parameters is the following: parseDate(year, month, day, "
          + "event_date, start_day_of_year, end_day_of_year). All of them are nullable. "
          + "Returns a row(year bigint, month bigint, day bigint, epoch_from bigint, epoch_to bigint).")
  @SqlType("row(year bigint, month bigint, day bigint, epoch_from bigint, epoch_to bigint)")
  public Block parseDate(
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice year,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice month,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice day,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice eventDate,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice startDayOfYear,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice endDayOfYear) {

    Long parsedYear = null;
    Long parsedMonth = null;
    Long parsedDay = null;
    Long parsedEpochFrom = null;
    Long parsedEpochTo = null;

    try {
      OccurrenceParseResult<IsoDateInterval> parsed =
          TemporalInterpreter.interpretRecordedDate(
              year != null ? year.toStringUtf8() : null,
              month != null ? month.toStringUtf8() : null,
              day != null ? day.toStringUtf8() : null,
              eventDate != null ? eventDate.toStringUtf8() : null,
              startDayOfYear != null ? startDayOfYear.toStringUtf8() : null,
              endDayOfYear != null ? endDayOfYear.toStringUtf8() : null);
      if (parsed.isSuccessful()) {
        IsoDateInterval dateRange = parsed.getPayload();
        if (dateRange.getTo() != null) {
          if (dateRange.getFrom().isSupported(ChronoField.YEAR)
              && dateRange.getFrom().get(ChronoField.YEAR)
                  == dateRange.getTo().get(ChronoField.YEAR)) {
            parsedYear = dateRange.getFrom().getLong(ChronoField.YEAR);
            if (dateRange.getFrom().isSupported(ChronoField.MONTH_OF_YEAR)
                && dateRange.getFrom().get(ChronoField.MONTH_OF_YEAR)
                    == dateRange.getTo().get(ChronoField.MONTH_OF_YEAR)) {
              parsedMonth = dateRange.getFrom().getLong(ChronoField.MONTH_OF_YEAR);
              if (dateRange.getFrom().isSupported(ChronoField.DAY_OF_MONTH)
                  && dateRange.getFrom().get(ChronoField.DAY_OF_MONTH)
                      == dateRange.getTo().get(ChronoField.DAY_OF_MONTH)) {
                parsedDay = dateRange.getFrom().getLong(ChronoField.DAY_OF_MONTH);
              }
            }
          }
        }
        if (dateRange.getFrom() != null) {
          parsedEpochFrom =
              TemporalAccessorUtils.toEarliestLocalDateTime(dateRange.getFrom(), true)
                  .toEpochSecond(ZoneOffset.UTC);
        }
        if (dateRange.getTo() != null) {
          parsedEpochTo =
              TemporalAccessorUtils.toLatestLocalDateTime(dateRange.getTo(), true)
                  .toEpochSecond(ZoneOffset.UTC);
        }
      }
    } catch (Exception e) {
      // not much to do - indicates bad data
      System.err.println(e.getMessage());
      e.printStackTrace();
    }

    RowType rowType =
        RowType.rowType(
            new RowType.Field(Optional.of("year"), BIGINT),
            new RowType.Field(Optional.of("month"), BIGINT),
            new RowType.Field(Optional.of("day"), BIGINT),
            new RowType.Field(Optional.of("epoch_from"), BIGINT),
            new RowType.Field(Optional.of("epoch_to"), BIGINT));
    RowBlockBuilder blockBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, 5);
    SingleRowBlockWriter builder = blockBuilder.beginBlockEntry();

    Consumer<Long> writer =
        v -> {
          if (v != null) {
            BIGINT.writeLong(builder, v);
          } else {
            builder.appendNull();
          }
        };

    writer.accept(parsedYear);
    writer.accept(parsedMonth);
    writer.accept(parsedDay);
    writer.accept(parsedEpochFrom);
    writer.accept(parsedEpochTo);

    blockBuilder.closeEntry();

    return blockBuilder.build().getObject(0, Block.class);
  }
}
