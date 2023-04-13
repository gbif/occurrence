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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ToISO8601Udf implements UDF1<String,String> {

  private transient final LoadingCache<String,String> cache;
  public ToISO8601Udf() {
    cache = CacheBuilder.newBuilder().maximumSize(100_000).build(new CacheLoader<String, String>() {
      @Override
      public String load(String key) throws Exception {
        return ToISO8601Udf.toIso8601(key);
      }
    });
  }

  private static String toIso8601(String value) {
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.ofEpochMilli(Long.parseLong(value)).atZone(ZoneOffset.UTC));
  }

  private static boolean isNotNullOrEmpty(String value) {
    return value != null && value.length() > 0;
  }

  @Override
  public String call(String field) throws Exception {
    return isNotNullOrEmpty(field)? cache.get(field) : null;
  }
}
