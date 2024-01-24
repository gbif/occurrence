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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A simple UDF for Hive that replaces specials characters with blanks. The characters replaced by
 * this UDF can break a download format and those are: tabs, line breaks and new lines. If the input
 * value is null or can't be parsed, an empty string is returned.
 */
@Description(name = "cleanDelimitersArray", value = "_FUNC_(field)")
public class CleanDelimiterArraysUDF extends UDF {

  public List<Text> evaluate(List<Text> field) {
    if (field == null) {
      return null;
    }

    return field.stream()
        .map(
            t ->
                new Text(
                    DownloadUtils.DELIMETERS_MATCH_PATTERN.matcher(t.toString()).replaceAll(" ")))
        .collect(Collectors.toList());
  }
}
