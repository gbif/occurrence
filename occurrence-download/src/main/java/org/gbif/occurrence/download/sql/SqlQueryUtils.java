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
package org.gbif.occurrence.download.sql;

import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlQueryUtils {

  /**
   * Replaces variables in the text. Variables come in a map of names and values.
   */
  private static String replaceVariables(String text, Map<String,String> params) {
    return StringUtils.replaceEach(text,
                                   params.keySet().stream().map(k -> "${" + k  + "}").toArray(String[]::new),
                                   params.values().toArray(new String[0]));
  }

  /**
   * Executes, statement-by-statement a String that contains multiple SQL queries.
   */
  public static void runMultiSQL(String queryDescription, String multiQuery, Map<String,String> params, BiConsumer<String, String> queryExecutor) {
    //Load parameters
    String filteredFileContent = replaceVariables(multiQuery, params);
    runMultiSQL(queryDescription, filteredFileContent, queryExecutor);
  }

  /**
   * Executes, statement-by-statement a String that contains multiple SQL queries.
   */
  public static void runMultiSQL(String queryDescription, String multiQuery, BiConsumer<String, String> queryExecutor) {
    for (String query : multiQuery.split(";\n")) {
      query = query.trim();
      if (!query.isEmpty()) {
        log.info("Executing query: \n {}", query);
        queryExecutor.accept(trimToLength(queryDescription + " " + cleanupQueryForDisplay(query), 200), query);
      }
    }
  }

  public static String cleanupQueryForDisplay(String input) {
    StringBuilder result = new StringBuilder();
    String[] lines = input.split("\n"); // Split the input into lines

    for (String line : lines) {
      // Check if the line starts with "--" or "<#--"
      if (!line.trim().startsWith("--") && !line.trim().startsWith("<#--")) {
        result.append(line).append("\n");
      }
    }
    return result.toString();
  }

  private static String trimToLength(String input, int maxLength) {
    if (input.length() > maxLength) {
      return input.substring(0, maxLength);
    }
    return input;
  }
}
