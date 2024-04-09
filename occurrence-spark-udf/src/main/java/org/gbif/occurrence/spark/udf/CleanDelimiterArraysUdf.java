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

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

public class CleanDelimiterArraysUdf implements UDF1<WrappedArray<String>,String[]> {

  private static final CleanDelimiters CLEAN_DELIMITERS = new CleanDelimiters();

  @Override
  public String[] call(WrappedArray<String> field) throws Exception {
    return field != null && !field.isEmpty()? toArray(field) : null;
  }

  /**
   * Converts to an array, returns null if the produced array is empty.
   */
  private String[] toArray(WrappedArray<String> field) {
    String[] value = JavaConverters.asJavaCollection(field).stream()
                      .map(CLEAN_DELIMITERS)
                      .filter(s -> s != null && !s.isEmpty())
                      .toArray(String[]::new);
    return value.length > 0? value : null;
  }
}
