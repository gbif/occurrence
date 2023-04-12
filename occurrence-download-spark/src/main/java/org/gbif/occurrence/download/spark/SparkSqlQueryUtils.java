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
package org.gbif.occurrence.download.spark;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;

import lombok.SneakyThrows;

@Slf4j
public class SparkSqlQueryUtils {

  public static SparkSession createSparkSession(String appName, String warehouseLocation) {
    return SparkSession.builder()
            .appName(appName)
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .enableHiveSupport().getOrCreate();
  }

  /**
   * Reads a file into a string.
   */
  @SneakyThrows
  private static String readFile(String fileName) {
    return new String(Files.readAllBytes(Paths.get(fileName)));
  }

  /**
   * Replaces variables in the text. Variables come in a map of names and values.
   */
  private static String replaceVariables(String text, Map<String,String> params) {
    return StringUtils.replaceEach(text,
                                   params.keySet().stream().map(k -> "${" + k  + "}").toArray(String[]::new),
                                   params.values().toArray(new String[0]));
  }

  /**
   * Executes, statement-by-statement a file containing SQL queries.
   */
  public static void runSQLFile(String fileName, Map<String,String> params, SparkSession sparkSession) {
    runMultiSQL(readFile(fileName), params, sparkSession);
  }

  /**
   * Executes, statement-by-statement a file containing SQL queries.
   */
  public static void runSQLFile(String fileName, SparkSession sparkSession) {
    runMultiSQL(readFile(fileName), sparkSession);
  }

  /**
   * Executes, statement-by-statement a  String that contains multiple SQL queries.
   */
  public static void runMultiSQL(String multiQuery, Map<String,String> params, SparkSession sparkSession) {
    //Load parameters
    String filteredFileContent = replaceVariables(multiQuery, params);
    runMultiSQL(filteredFileContent, sparkSession);
  }

  /**
   * Executes, statement-by-statement a  String that contains multiple SQL queries.
   */
  public static void runMultiSQL(String multiQuery, SparkSession sparkSession) {
    for (String query : multiQuery.split(";")) {
      log.info("Executing query: \n {}", query);
      sparkSession.sql(query);
    }
  }
}
