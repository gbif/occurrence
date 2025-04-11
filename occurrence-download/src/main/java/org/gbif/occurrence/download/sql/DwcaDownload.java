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

import org.gbif.api.model.occurrence.Download;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;
import org.gbif.occurrence.download.hive.ExtensionsQuery;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;
import org.gbif.occurrence.download.util.DownloadRequestUtils;

import java.io.StringWriter;
import java.util.Map;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public class DwcaDownload {

  private static String downloadQuery;

  private static String dropTablesQuery;

  private final Supplier<SparkQueryExecutor> queryExecutorSupplier;

  private final Download download;

  private final DownloadQueryParameters queryParameters;

  private final WorkflowConfiguration workflowConfiguration;

  private final DownloadStage downloadStage;

  public void run() {
    switch (downloadStage) {
      case QUERY:
        executeQuery();
        break;
      case ARCHIVE:
        zipAndArchive();
        break;
      case ALL:
    try {
      executeQuery();
      zipAndArchive();
        } finally {
          DownloadCleaner.dropTables(download.getKey(), workflowConfiguration);
        }
        break;
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
