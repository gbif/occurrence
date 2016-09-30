package org.gbif.occurrence.hive.udf;

import org.gbif.occurrence.common.download.DownloadUtils;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A simple UDF for Hive that replaces specials characters with blanks.
 * The characters replaced by this UDF can break a download format and those are: tabs, line breaks and new lines.
 * If the input value is null or can't be parsed, and empty string is returned.
 */
@Description(
  name = "cleanDelimiters",
  value = "_FUNC_(field)")
public class CleanDelimiterCharsUDF extends UDF {

  private final Text text = new Text();

  public Text evaluate(Text field) {
    if (field == null) {
      return null;
    }
    text.set(DownloadUtils.DELIMETERS_MATCH_PATTERN.matcher(field.toString()).replaceAll(" "));
    return text;
  }
}
