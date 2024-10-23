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
package org.gbif.occurrence.spark.udf;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.spark.sql.api.java.UDF1;

/**
 * A simple UDF for Hive to convert an epoch time in seconds to a string in ISO 8601 format, without zone information.
 * If the input value is null or can't be parsed, null is returned.
 */
public class SecondsToLocalISO8601Udf implements UDF1<Long,String> {

  private static String secondsToLocalIso8601(Long value) {
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.ofEpochSecond(value).atZone(ZoneOffset.UTC));
  }

  @Override
  public String call(Long field) throws Exception {
    return field != null ? secondsToLocalIso8601(field) : null;
  }
}
