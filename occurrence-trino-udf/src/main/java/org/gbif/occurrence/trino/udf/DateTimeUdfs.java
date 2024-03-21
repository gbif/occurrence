package org.gbif.occurrence.trino.udf;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DateTimeUdfs {

  @ScalarFunction(value = "toISO8601", deterministic = true)
  @Description("Converts a timestamp to a date time")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static Slice toISO8601(@SqlNullable @SqlType(StandardTypes.BIGINT) Long value) {
    return value != null
        ? Slices.utf8Slice(
            DateTimeFormatter.ISO_INSTANT.format(
                Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC)))
        : null;
  }

  @ScalarFunction(value = "toLocalISO8601", deterministic = true)
  @Description("Converts a timestamp to a local date time")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static Slice toLocalISO8601(@SqlNullable @SqlType(StandardTypes.BIGINT) Long value) {
    return value != null
        ? Slices.utf8Slice(
            DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC)))
        : null;
  }
}
