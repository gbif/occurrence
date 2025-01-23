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
import java.util.Map;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;
import org.gbif.occurrence.download.hive.ExtensionsQuery;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.download.spark.GbifOccurrenceDownloads;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;
import org.gbif.occurrence.download.util.DownloadRequestUtils;

@Builder
@Slf4j
public class DwcaDownload {

  private static String downloadQuery;

  private static String dropTablesQuery;

  private final Supplier<SparkQueryExecutor> queryExecutorSupplier;

  private final Download download;

  private final DownloadQueryParameters queryParameters;

  private final WorkflowConfiguration workflowConfiguration;

  public void run() {
    try {
      // Execute queries
      executeQuery();

      // Create the Archive

      zipAndArchive();

    } finally {
      // Drop tables
      dropTables();
    }
  }

  private void executeQuery() {

    try (SparkQueryExecutor queryExecutor = queryExecutorSupplier.get()) {

      Map<String, String> queryParams = getQueryParameters();
      SqlQueryUtils.runMultiSQL("Initial DWCA Download query", downloadQuery(), queryParams, queryExecutor);

      if (DownloadRequestUtils.hasVerbatimExtensions(download.getRequest())) {
        runExtensionsQuery(queryExecutor);
      }
    }
  }

  @SneakyThrows
  private String downloadQuery() {
    if (downloadQuery == null) {
      downloadQuery = SqlQueryUtils.queryTemplateToString(GenerateHQL::generateDwcaQueryHQL);
    }
    return downloadQuery;
  }

  @SneakyThrows
  private String dropTablesQuery() {
    if (dropTablesQuery == null) {
      dropTablesQuery =
          SqlQueryUtils.queryTemplateToString(GenerateHQL::generateDwcaDropTableQueryHQL);
    }
    return dropTablesQuery;
  }

  private void zipAndArchive() {
    DownloadJobConfiguration configuration =
        DownloadJobConfiguration.builder()
            .downloadKey(download.getKey())
            .downloadTableName(queryParameters.getDownloadTableName())
            .filter(queryParameters.getWhereClause())
            .isSmallDownload(Boolean.FALSE)
            .sourceDir(workflowConfiguration.getHiveDBPath())
            .downloadFormat(workflowConfiguration.getDownloadFormat())
            .coreTerm(download.getRequest().getType().getCoreTerm())
            .extensions(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
            .build();
    DwcaArchiveBuilder.of(configuration, workflowConfiguration).buildArchive();
  }

  private void dropTables() {
    try (SparkQueryExecutor queryExecutor = getSingleQueryExecutor()) {
      SqlQueryUtils.runMultiSQL("Drop tables - DWCA Download", dropTablesQuery(), queryParameters.toMap(), queryExecutor);
    }
  }

  /**
   * Create a single query executor for dropping tables.
   */
  private SparkQueryExecutor getSingleQueryExecutor() {
    return SparkQueryExecutor.createSingleQueryExecutor("Clean-up Download job " + download.getKey(), workflowConfiguration);
  }

  private Map<String, String> getQueryParameters() {
    Map<String, String> parameters = queryParameters.toMap();
    parameters.put("verbatimTable", queryParameters.getDownloadTableName() + "_verbatim");
    parameters.put("interpretedTable", queryParameters.getDownloadTableName() + "_interpreted");
    parameters.put("citationTable", queryParameters.getDownloadTableName() + "_citation");
    parameters.put("multimediaTable", queryParameters.getDownloadTableName() + "_multimedia");

    return parameters;
  }

  private void runExtensionsQuery(SparkQueryExecutor sparkQueryExecutor) {
    SqlQueryUtils.runMultiSQL("Extensions DWCA Download query", extensionQuery(), queryParameters.toMap(), sparkQueryExecutor);
  }

  @SneakyThrows
  private String extensionQuery() {
    try (StringWriter writer = new StringWriter()) {
      ExtensionsQuery.builder().writer(writer).build().generateExtensionsQueryHQL(download);
      return writer.toString();
    }
  }
}
