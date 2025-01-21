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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.occurrence.Download;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;
import org.gbif.occurrence.download.hive.ExtensionsQuery;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.download.util.DownloadRequestUtils;

@Builder
@Slf4j
public class DwcaDownload {

  private static String downloadQuery;

  private static String dropTablesQuery;

  private final QueryExecutor queryExecutor;

  private final Download download;

  private final DownloadQueryParameters queryParameters;

  private final WorkflowConfiguration workflowConfiguration;

  private final SparkSessionSupplier sparkSessionSupplier;

  public void run() throws IOException {
    try {
      // Execute queries
      log.info("Running DWCA download for download key {}", download.getKey());
      executeQuery();

      log.info("Zipping and archiving DWCA download for download key {}", download.getKey());
      zipAndArchive();

    } finally {
      // Drop tables
      log.info("Dropping tables for download key {}", download.getKey());
      dropTables();
    }
  }

  private void executeQuery() throws IOException {

    Map<String,String> queryParams = getQueryParameters();
    SqlQueryUtils.runMultiSQL("Initial DWCA Download query", downloadQuery(), queryParams, queryExecutor);

    //Citation table
    createCitationTable(queryParams.get("hiveDB"), queryParams.get("interpretedTable"), queryParams.get("citationTable"));

    if (DownloadRequestUtils.hasVerbatimExtensions(download.getRequest())) {
      runExtensionsQuery();
    }

    queryExecutor.close();
  }

  /**
   * Creates the citation table.
   */
  private void createCitationTable(String database, String interpretedTable, String citationTable) {
    SparkSession sparkSession = sparkSessionSupplier.create();
    sparkSession.sparkContext().setJobGroup("CT", "Creating citation table", true);
    sparkSession.sql("SET hive.auto.convert.join=true");
    sparkSession.sql("SET mapred.output.compress=false");
    sparkSession.sql("SET hive.exec.compress.output=false");
    Dataset<Row> result = sparkSession.sql("SELECT datasetkey, count(*) as num_occurrences FROM " +
      database + '.' + interpretedTable + " WHERE datasetkey IS NOT NULL GROUP BY datasetkey");
    result.coalesce(1).write().option("delimiter", "\t").format("hive").saveAsTable(database + '.' + citationTable);
    sparkSession.sparkContext().clearJobGroup();
    sparkSession.close();
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
    SqlQueryUtils.runMultiSQL("Drop tables - DWCA Download",
      dropTablesQuery(), queryParameters.toMap(), queryExecutor);
  }

  private Map<String, String> getQueryParameters() {
    Map<String, String> parameters = queryParameters.toMap();
    parameters.put("verbatimTable", queryParameters.getDownloadTableName() + "_verbatim");
    parameters.put("interpretedTable", queryParameters.getDownloadTableName() + "_interpreted");
    parameters.put("citationTable", queryParameters.getDownloadTableName() + "_citation");
    parameters.put("multimediaTable", queryParameters.getDownloadTableName() + "_multimedia");

    return parameters;
  }

  private void runExtensionsQuery() {
    SqlQueryUtils.runMultiSQL("Extensions DWCA Download query", extensionQuery(), queryParameters.toMap(), queryExecutor);
  }

  @SneakyThrows
  private String extensionQuery() {
    try (StringWriter writer = new StringWriter()) {
      ExtensionsQuery.builder().writer(writer).build().generateExtensionsQueryHQL(download);
      return writer.toString();
    }
  }
}
