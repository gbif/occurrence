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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.gbif.occurrence.common.download.DownloadUtils;

import java.time.Instant;
import java.time.ZoneOffset;

/**
 * A simple UDF for Hive to convert a millisecond date/long to a string in ISO 8601 format.
 * If the input value is null or can't be parsed, an empty string is returned.
 */
@Description(
  name = "toISO8601",
  value = "_FUNC_(field)")
public class ToISO8601MillisUDF extends UDF {

  private final Text text = new Text();

  public Text evaluate(Text field) {
    if (field == null || field.getLength() == 0) {
      return null;
    } else {
      try {
        text.set(DownloadUtils.ISO_8601_ZONED.format(Instant.ofEpochMilli(Long.parseLong(field.toString())).atZone(ZoneOffset.UTC)));
        return text;
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }
}
