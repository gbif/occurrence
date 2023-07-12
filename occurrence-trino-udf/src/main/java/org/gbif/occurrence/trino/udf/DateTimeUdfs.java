package org.gbif.occurrence.trino.udf;

import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import lombok.experimental.UtilityClass;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@UtilityClass
public class DateTimeUdfs {

  @ScalarFunction(value = "toISO8601", deterministic = true)
  @Description("Converts a timestamp to a date time")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static String toISO8601(@SqlNullable @SqlType(StandardTypes.INTEGER) Long value) {
    return value != null? DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC)) : null;
  }

  @ScalarFunction(value = "toLocalISO8601", deterministic = true)
  @Description("Converts a timestamp to a local date time")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static String toLocalISO8601(@SqlNullable @SqlType(StandardTypes.INTEGER) Long value) {
    return value != null? DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC)) : null;
  }
}
