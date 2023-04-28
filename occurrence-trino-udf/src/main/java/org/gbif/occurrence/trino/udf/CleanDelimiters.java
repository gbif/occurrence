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
package org.gbif.occurrence.trino.udf;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import lombok.SneakyThrows;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

class CleanDelimiters implements Function<Slice,Slice> {

  private final Map<Slice,Slice> cache;
  public CleanDelimiters() {
    cache = createLRUMap(100_00);
  }

  private static Slice cleanDelimiters(Slice value) {
    return Slices.utf8Slice(DELIMETERS_MATCH_PATTERN.matcher(value.toString()).replaceAll(" ").trim());
  }

  public static final String DELIMITERS_MATCH =
    "\\t|\\n|\\r|(?:(?>\\u000D\\u000A)|[\\u000A\\u000B\\u000C\\u000D\\u0085\\u2028\\u2029\\u0000])";

  public static final Pattern DELIMETERS_MATCH_PATTERN = Pattern.compile(DELIMITERS_MATCH);
  @Override
  @SneakyThrows
  public Slice apply(Slice value) {
    return value != null? cache.computeIfAbsent(value, CleanDelimiters::cleanDelimiters) : null;
  }

  public static <K, V> Map<K, V> createLRUMap(final int maxEntries) {
    return new LinkedHashMap<K, V>(maxEntries*10/7, 0.7f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxEntries;
      }
    };
  }
}
