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

import java.util.regex.Pattern;

import org.apache.spark.sql.api.java.UDF3;

import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

/**
 * Equivalent to a LIKE or ILIKE operator on a string array.
 */
public class StringArrayLikeGenericUdf implements UDF3<WrappedArray<String>, String, Boolean, Boolean> {

  @Override
  public Boolean call(WrappedArray<String> array, String pattern, Boolean caseSensitive) throws Exception {
    Pattern regex = sqlLikeToRegex(pattern, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
    return array != null && !array.isEmpty() && JavaConverters.asJavaCollection(array).stream()
                                                  .anyMatch(e -> e != null && (regex.matcher(e).matches()));
  }

  /**
   * Convert a basic wildcard pattern with ? and * into a regular expression.
   */
  private Pattern sqlLikeToRegex(String sqlLike, int flags) {
    char[] patternChars = sqlLike.toCharArray();
    StringBuilder regex = new StringBuilder("^\\Q");
    int i = 0;
    while (i < sqlLike.length()) {
      if (patternChars[i] == '\\') {
        // Only include an escape for the following character if it's ? or *.
        char next = patternChars[++i];
        if (next == '?' || next == '*') {
          // No need to escape as we are within a regex quote.
          regex.append(next);
        } else {
          // No need to escape...
          regex.append(next);
        }
      } else if (patternChars[i] == '?') {
        regex.append("\\E.\\Q");
      } else if (patternChars[i] == '*') {
        regex.append("\\E.*\\Q");
      } else {
        regex.append(patternChars[i]);
      }
      i++;
    }
    regex.append("\\E$");

    return Pattern.compile(regex.toString(), flags);
  }
}
