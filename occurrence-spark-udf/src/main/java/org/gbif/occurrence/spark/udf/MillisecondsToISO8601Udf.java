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
 * A simple UDF for Hive to convert an epoch time in milliseconds to a string in ISO 8601 format with zone information.
 * If the input value is null or can't be parsed, null is returned.
 */
public class MillisecondsToISO8601Udf implements UDF1<Long,String> {

  private static String millisToISO8601Udf(Long value) {
    return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(Long.parseLong(value.toString())).atZone(ZoneOffset.UTC));
  }

  @Override
  public String call(Long field) throws Exception {
    return field != null ? millisToISO8601Udf(field): null;
  }
}
