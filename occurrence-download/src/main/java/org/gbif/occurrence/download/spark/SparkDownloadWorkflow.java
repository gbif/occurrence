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

import static org.gbif.occurrence.download.util.VocabularyUtils.*;

import java.util.function.Supplier;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.sql.*;
import org.gbif.vocabulary.client.ConceptClient;

@Slf4j
public class SparkDownloadWorkflow {

  private final WorkflowConfiguration workflowConfiguration;
  private final String downloadKey;
  private final DwcTerm coreDwcTerm;
  private final Supplier<SparkQueryExecutor> queryExecutorSupplier;
  private final DownloadStage downloadStage;

  @Builder
  public SparkDownloadWorkflow(
      WorkflowConfiguration workflowConfiguration,
      DwcTerm coreDwcTerm,
      String downloadKey,
      Supplier<SparkQueryExecutor> queryExecutorSupplier,
      DownloadStage downloadStage) {
    this.workflowConfiguration = workflowConfiguration;
    this.downloadKey = downloadKey;
    this.coreDwcTerm = coreDwcTerm;
    this.queryExecutorSupplier = queryExecutorSupplier;
    this.downloadStage = downloadStage;
  }

  public void run() {
    if (DownloadStage.CLEANUP != downloadStage) {
      Download download = getDownload();

      DownloadJobConfiguration jobConfiguration =
          DownloadJobConfiguration.forSqlDownload(download, workflowConfiguration);

      DownloadQueryParameters queryParameters =
          DownloadQueryParameters.from(download, jobConfiguration, workflowConfiguration);

      if (isStage(DownloadStage.QUERY)) {
        DownloadQueryRunner.builder()
            .download(download)
            .queryExecutorSupplier(queryExecutorSupplier)
            .queryParameters(queryParameters)
            .build()
            .runDownloadQuery();
      }

      if (isStage(DownloadStage.ARCHIVE)) {
        DownloadArchiver.builder()
            .queryParameters(queryParameters)
            .workflowConfiguration(workflowConfiguration)
            .download(download)
            .build()
            .archive();
      }
    }

    if (isStage(DownloadStage.CLEANUP)) {
      DownloadCleaner.dropTables(downloadKey, workflowConfiguration);
    }
  }

  private Download getDownload() {
    OccurrenceDownloadService downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    Download download = downloadService.get(downloadKey);
    ConceptClient conceptClient = DownloadWorkflowModule.conceptClient(workflowConfiguration);
    if (DwcTerm.Event == coreDwcTerm) {
      translateEventPredicateFields(download, conceptClient);
    } else {
      translateOccurrencePredicateFields(download, conceptClient);
    }
    return download;
  }

  private boolean isStage(DownloadStage stage) {
    return downloadStage == stage || downloadStage == DownloadStage.ALL;
  }
}
