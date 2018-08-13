package org.gbif.occurrence.hive.udf;

import org.gbif.occurrence.common.download.DownloadUtils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A simple UDF for Hive to convert a date/long to an string in ISO 8601 format.
 * If the input value is null or can't be parsed, and empty string is returned.
 */
@Description(
  name = "toISO8601",
  value = "_FUNC_(field)")
public class ToISO8601UDF extends UDF {

  private final Text text = new Text();

  public Text evaluate(Text field) {
    if (field == null) {
      text.set("");
    } else {
      try {
        text.set(DownloadUtils.ISO_8601_FORMAT.format(Instant.ofEpochMilli(Long.parseLong(field.toString())).atZone(ZoneOffset.UTC)));
      } catch (NumberFormatException e) {
        text.set("");
      }
    }

    return text;
  }
}
