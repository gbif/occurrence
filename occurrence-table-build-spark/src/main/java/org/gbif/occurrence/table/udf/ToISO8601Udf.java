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
package org.gbif.occurrence.table.udf;

import org.gbif.occurrence.common.download.DownloadUtils;

import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.spark.sql.api.java.UDF1;

public class ToISO8601Udf implements UDF1<String,String> {

  @Override
  public String call(String field) throws Exception {
    if (field == null || field.length() == 0) {
      return null;
    } else {
      try {
        return DownloadUtils.ISO_8601_LOCAL.format(Instant.ofEpochMilli(Long.parseLong(field.toString())).atZone(ZoneOffset.UTC));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }
}
