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
import org.cache2k.Cache;

public class ToLocalISO8601Udf implements UDF1<Long,String> {

  private final Cache<Long,String> cache = UDFS.createLRUMap(100_000, ToLocalISO8601Udf::toLocalIso8601);

  private static String toLocalIso8601(Long value) {
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC));
  }

  @Override
  public String call(Long field) throws Exception {
    return field != null? cache.computeIfAbsent(field, ToLocalISO8601Udf::toLocalIso8601) : null;
  }
}
