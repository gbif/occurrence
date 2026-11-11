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
package org.gbif.occurrence.search;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Response of a batch lookup of records by key: the records found, in the order of the requested
 * keys, and the keys that weren't found.
 *
 * @param <K> key type
 * @param <T> record type
 */
public class BatchResponse<K, T> {

  private final List<T> results;
  private final List<K> notFound;

  public BatchResponse(List<T> results, List<K> notFound) {
    this.results = results;
    this.notFound = notFound;
  }

  /**
   * Response of the requested keys from the records found by key.
   *
   * @param keys requested keys, duplicates are returned once
   * @param records records found, by key
   */
  public static <K, T> BatchResponse<K, T> of(List<K> keys, Map<K, T> records) {
    List<K> distinct = distinctKeys(keys);
    return new BatchResponse<>(
        distinct.stream().map(records::get).filter(Objects::nonNull).toList(),
        distinct.stream().filter(k -> !records.containsKey(k)).toList());
  }

  /**
   * Requested keys without nulls nor duplicates, in their original order.
   *
   * @throws IllegalArgumentException if there are more than {@code maxKeys} distinct keys
   */
  public static <K> List<K> distinctKeys(List<K> keys, int maxKeys) {
    List<K> distinct = distinctKeys(keys);
    Preconditions.checkArgument(
        distinct.size() <= maxKeys,
        "Max number of keys of " + maxKeys + " exceeded: " + distinct.size());
    return distinct;
  }

  private static <K> List<K> distinctKeys(List<K> keys) {
    return keys == null
        ? List.of()
        : keys.stream().filter(Objects::nonNull).distinct().toList();
  }

  public List<T> getResults() {
    return results;
  }

  public List<K> getNotFound() {
    return notFound;
  }
}
