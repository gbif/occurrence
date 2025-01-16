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

import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.util.SqlValidation;
import org.gbif.occurrence.query.sql.HiveSqlQuery;

@Slf4j
public class SqlQueryUtils {

  /** Reads a file into a string. */
  @SneakyThrows
  private static String readFile(String fileName) {
    return new String(Files.readAllBytes(Paths.get(fileName)));
  }

  /** Replaces variables in the text. Variables come in a map of names and values. */
  private static String replaceVariables(String text, Map<String, String> params) {
    return StringUtils.replaceEach(
        text,
        params.keySet().stream().map(k -> "${" + k + "}").toArray(String[]::new),
        params.values().toArray(new String[0]));
  }

  /** Executes, statement-by-statement a file containing SQL queries. */
  public static void runSQLFile(
      String fileName, Map<String, String> params, Consumer<String> queryExecutor) {
    runMultiSQL(readFile(fileName), params, queryExecutor);
  }

  /** Executes, statement-by-statement a String that contains multiple SQL queries. */
  public static void runMultiSQL(
      String multiQuery, Map<String, String> params, Consumer<String> queryExecutor) {
    // Load parameters
    String filteredFileContent = replaceVariables(multiQuery, params);
    runMultiSQL(filteredFileContent, queryExecutor);
  }

  /** Executes, statement-by-statement a String that contains multiple SQL queries. */
  public static void runMultiSQL(String multiQuery, Consumer<String> queryExecutor) {
    for (String query : multiQuery.split(";\n")) {
      query = query.trim();
      if (!query.isEmpty()) {
        log.info("Executing query: \n {}", query);
        queryExecutor.accept(query);
      }
    }
  }

  @FunctionalInterface
  public interface TemplateConsumer<T> {
    void accept(T t) throws Exception;
  }

  @SneakyThrows
  public static String queryTemplateToString(TemplateConsumer<Writer> templateBuilder) {
    try (StringWriter stringWriter = new StringWriter()) {
      templateBuilder.accept(stringWriter);
      return stringWriter.toString();
    }
  }

  @SneakyThrows
  public static DownloadQueryParameters downloadQueryParameters(
      DownloadJobConfiguration jobConfiguration,
      WorkflowConfiguration workflowConfiguration,
      Download download) {
    DownloadQueryParameters.DownloadQueryParametersBuilder builder =
        DownloadQueryParameters.builder()
            .downloadTableName(jobConfiguration.getDownloadTableName())
            .whereClause(jobConfiguration.getFilter())
            .tableName(jobConfiguration.getCoreTerm().name().toLowerCase())
            .database(workflowConfiguration.getHiveDb())
            .warehouseDir(workflowConfiguration.getHiveWarehouseDir());
    if (DownloadFormat.SQL_TSV_ZIP == jobConfiguration.getDownloadFormat()) {
      SqlValidation sv = new SqlValidation(workflowConfiguration.getHiveDb());

      String userSql = ((SqlDownloadRequest) download.getRequest()).getSql();
      HiveSqlQuery sqlQuery =
          sv.validateAndParse(
              userSql); // Declares QueryBuildingException but it's already been validated.
      builder
          .userSql(sqlQuery.getSql())
          .userSqlHeader(String.join("\t", sqlQuery.getSqlSelectColumnNames()))
          .whereClause(sqlQuery.getSqlWhere());
    }
    return builder.build();
  }
}
