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

import java.util.ArrayList;
import java.util.List;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import lombok.experimental.UtilityClass;

import static io.trino.spi.type.VarcharType.VARCHAR;

@UtilityClass
public class DataCleanUdfs {

  private static final CleanDelimiters CLEAN_DELIMITERS = new CleanDelimiters();

  @ScalarFunction(value = "cleanDelimitersArray", deterministic = true)
  @Description(
      "Removes delimiter characters of each element of the input array. E.g.:  select cleanDelimitersArray(ARRAY['a','b']);")
  @SqlType("array(varchar)")
  @SqlNullable
  public static Block cleanDelimitersArray(@SqlType("array(varchar)") Block value) {
    if (value == null || value.isNull(0)) {
      return null;
    }

    BlockBuilder builder = VARCHAR.createBlockBuilder(null, value.getPositionCount());
    for (int i = 0; i < value.getPositionCount(); i++) {
      Slice v = VARCHAR.getSlice(value, i);
      VARCHAR.writeSlice(builder, CLEAN_DELIMITERS.apply(v));
    }

    return builder.build();
  }

  @ScalarFunction(value = "cleanDelimiters", deterministic = true)
  @Description("Removes delimiter characters of an input string")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static Slice cleanDelimiters(@SqlType(StandardTypes.VARCHAR) Slice value) {
    return CLEAN_DELIMITERS.apply(value);
  }

  @ScalarFunction(value = "removeNullsStringArray", deterministic = true)
  @Description("Removes all null values from a list of strings")
  @SqlType("array(varchar)")
  @SqlNullable
  public static Block removeNullsStringArray(@SqlType("array(varchar)") Block values) {
    if (values == null || values.isNull(0)) {
      return null;
    }

    List<Slice> nonNullValues = new ArrayList<>();
    for (int i = 0; i < values.getPositionCount(); i++) {
      Slice v = VARCHAR.getSlice(values, i);
      if (v != null && v.toStringUtf8() != null && !v.toStringUtf8().trim().isEmpty()) {
        nonNullValues.add(v);
      }
    }

    BlockBuilder builder = VARCHAR.createBlockBuilder(null, nonNullValues.size());
    nonNullValues.forEach(v -> VARCHAR.writeSlice(builder, v));

    return builder.build();
  }

  @ScalarFunction(value = "stringArrayContains", deterministic = true)
  @Description("Checks if an array contains an element")
  @SqlType(StandardTypes.BOOLEAN)
  public static boolean stringArrayContains(
      @SqlType("array(varchar)") Block values,
      @SqlType(StandardTypes.VARCHAR) Slice value,
      @SqlType(StandardTypes.BOOLEAN) boolean caseSensitive) {
    if (values == null || values.isNull(0)) {
      return false;
    }

    for (int i = 0; i < values.getPositionCount(); i++) {
      Slice v = VARCHAR.getSlice(values, i);
      if (caseSensitive && value.toStringUtf8().equals(v.toStringUtf8())) {
        return true;
      } else if (!caseSensitive && value.toStringUtf8().equalsIgnoreCase(v.toStringUtf8())) {
        return true;
      }
    }

    return false;
  }
}
