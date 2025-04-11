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

import static org.gbif.occurrence.download.util.VocabularyUtils.*;

import java.util.function.Supplier;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;
import org.gbif.occurrence.download.util.SqlValidation;
import org.gbif.occurrence.query.sql.HiveSqlQuery;
import org.gbif.vocabulary.client.ConceptClient;

@Slf4j
public class DownloadWorkflow {

  private final DownloadJobConfiguration jobConfiguration;

  private final WorkflowConfiguration workflowConfiguration;

  private final Download download;

  private final Supplier<SparkQueryExecutor> queryExecutorSupplier;

  private final DownloadStage downloadStage;

  @Builder
  public DownloadWorkflow(
      WorkflowConfiguration workflowConfiguration,
      DwcTerm coreDwcTerm,
      String downloadKey,
      Supplier<SparkQueryExecutor> queryExecutorSupplier,
      DownloadStage downloadStage) {
    this.workflowConfiguration = workflowConfiguration;
    OccurrenceDownloadService downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    download = downloadService.get(downloadKey);
    ConceptClient conceptClient = DownloadWorkflowModule.conceptClient(workflowConfiguration);
    translateVocabs(download, conceptClient);
    this.queryExecutorSupplier = queryExecutorSupplier;
    this.jobConfiguration =
        DownloadJobConfiguration.forSqlDownload(download, workflowConfiguration.getHiveDBPath());
    this.downloadStage = downloadStage;
  }

  public void run() {
    if (DownloadStage.CLEANUP != downloadStage) {
      if (download.getRequest().getFormat() == DownloadFormat.DWCA) {
        DwcaDownload.builder()
            .download(download)
            .downloadStage(downloadStage)
            .workflowConfiguration(workflowConfiguration)
            .queryParameters(downloadQueryParameters(jobConfiguration, workflowConfiguration))
            .queryExecutorSupplier(queryExecutorSupplier)
            .build()
            .run();
      } else {
        SimpleDownload.builder()
            .download(download)
            .downloadStage(downloadStage)
            .queryParameters(downloadQueryParameters(jobConfiguration, workflowConfiguration))
            .workflowConfiguration(workflowConfiguration)
            .sparkQueryExecutorSupplier(queryExecutorSupplier)
            .build()
            .run();
      }
    }
  }

  @SneakyThrows
  private DownloadQueryParameters downloadQueryParameters(
      DownloadJobConfiguration jobConfiguration, WorkflowConfiguration workflowConfiguration) {
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
              userSql, true); // Declares QueryBuildingException but it's already been validated.
      builder
          .userSql(sqlQuery.getSql())
          .userSqlHeader(String.join("\t", sqlQuery.getSqlSelectColumnNames()))
          .whereClause(sqlQuery.getSqlWhere());
    }
    return builder.build();
  }
}
