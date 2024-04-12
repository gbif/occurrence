package org.gbif.occurrence.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ToISO8601MillisUdf implements UDF1<Long,String> {

  private static String toISO8601MillisUdf(Long value) {
    return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(Long.parseLong(value.toString())).atZone(ZoneOffset.UTC));
  }

  @Override
  public String call(Long field) throws Exception {
    return field != null?  toISO8601MillisUdf(field): null;
  }
}
