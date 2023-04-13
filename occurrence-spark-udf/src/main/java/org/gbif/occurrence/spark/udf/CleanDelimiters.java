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


import java.util.function.Function;
import java.util.regex.Pattern;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import lombok.SneakyThrows;

public class CleanDelimiters implements Function<String,String> {

  private final LoadingCache<String,String> cache;
  public CleanDelimiters() {
    cache = CacheBuilder.newBuilder().maximumSize(100_000).build(CacheLoader.from(CleanDelimiters::cleanDelimiters));
  }

  private static String cleanDelimiters(String value) {
    return DELIMETERS_MATCH_PATTERN.matcher(value).replaceAll(" ").trim();
  }

  public static final String DELIMETERS_MATCH =
    "\\t|\\n|\\r|(?:(?>\\u000D\\u000A)|[\\u000A\\u000B\\u000C\\u000D\\u0085\\u2028\\u2029\\u0000])";

  public static final Pattern DELIMETERS_MATCH_PATTERN = Pattern.compile(DELIMETERS_MATCH);
  @Override
  @SneakyThrows
  public String apply(String value) {
    return value != null? cache.get(value) : null;
  }
}
