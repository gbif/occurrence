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

import org.gbif.occurrence.common.download.DownloadUtils;

import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A simple UDF for Hive to convert a date/long to an string in ISO 8601 format, without zone information.
 * If the input value is null or can't be parsed, and empty string is returned.
 */
@Description(
  name = "toLocalISO8601",
  value = "_FUNC_(field)")
public class ToLocalISO8601UDF extends UDF {

  private final Text text = new Text();

  public Text evaluate(Text field) {
    if (field == null || field.getLength() == 0) {
      return null;
    } else {
      try {
        text.set(DownloadUtils.ISO_8601_LOCAL.format(Instant.ofEpochSecond(Long.parseLong(field.toString())).atZone(ZoneOffset.UTC)));
        return text;
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }
}
