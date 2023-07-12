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
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;
import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.Objects;

@UtilityClass
public class DataCleanUdfs {

  private static final CleanDelimiters CLEAN_DELIMITERS = new CleanDelimiters();

  @ScalarFunction(value = "cleanDelimitersArray", deterministic = true)
  @Description("Removes delimiter characters of each element of the input array")
  @SqlType(StandardTypes.ARRAY)
  @SqlNullable
  public static Slice[] cleanDelimitersArray(@SqlType(StandardTypes.ARRAY) Slice[] value) {
    return value != null? Arrays.stream(value)
      .map(CLEAN_DELIMITERS)
      .filter(s -> s != null && s.length() > 0)
      .toArray(Slice[]::new) : null;
  }


  @ScalarFunction(value = "cleanDelimiters", deterministic = true)
  @Description("Removes delimiter characters of an input string")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static Slice cleanDelimiters(@SqlType(StandardTypes.VARCHAR) Slice value)  {
    return CLEAN_DELIMITERS.apply(value);
  }

  private static String[] toStringArray(Slice[] slices) {
    return Arrays.stream(slices).map(Slice::toStringUtf8).toArray(String[]::new);
  }

  @ScalarFunction(value = "joinArray", deterministic = true)
  @Description("Joins all the elements of an array using a separator")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static Slice joinArray(@SqlType(StandardTypes.ARRAY) Slice[] value, @SqlType(StandardTypes.VARCHAR) Slice sep) {
    return (value != null && value.length > 0)? Slices.utf8Slice(String.join(sep.toStringUtf8(), toStringArray(value))) : null;
  }

  @ScalarFunction(value = "removeNulls", deterministic = true)
  @Description("Removes all null values from a list")
  @SqlType(StandardTypes.ARRAY)
  @SqlNullable
  public static Object[] removeNulls(@SqlType(StandardTypes.ARRAY) Objects[] values) {
    return values == null || values.length == 0 ? null : Arrays.stream(values).filter(Objects::nonNull).toArray(Object[]::new);
  }


  @ScalarFunction(value = "stringArrayContains", deterministic = true)
  @Description("Checks if an array contains an element")
  @SqlType(StandardTypes.BOOLEAN)
  public static boolean stringArrayContains(@SqlType(StandardTypes.ARRAY) Slice[] values, @SqlType(StandardTypes.VARCHAR) String value, @SqlType(StandardTypes.BOOLEAN) boolean caseSensitive) {
    return values != null && values.length > 0 && Arrays.stream(values).map(Slice::toStringUtf8).anyMatch(e -> caseSensitive? e.equals(value) : e.equalsIgnoreCase(value));
  }

}
