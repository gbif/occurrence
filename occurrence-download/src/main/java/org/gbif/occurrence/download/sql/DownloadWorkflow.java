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

import java.util.Properties;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.action.FromSearchDownloadAction;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.elastic.DownloadEsClient;
import org.gbif.occurrence.download.predicate.EsPredicateUtil;
import org.gbif.occurrence.download.util.DownloadRequestUtils;
import org.gbif.occurrence.search.es.VocabularyFieldTranslator;
import org.gbif.vocabulary.client.ConceptClient;

@Slf4j
public class DownloadWorkflow {

  // arbitrary record count that represents and error counting the records of the input query
  private static final int ERROR_COUNT = -1;

  private final OccurrenceDownloadService downloadService;
  private final ConceptClient conceptClient;

  private final DwcTerm coreDwcTerm;

  private final WorkflowConfiguration workflowConfiguration;

  private final Download download;

  private final SqlDownloadRunner sqlDownloadRunner;

  @Builder
  public DownloadWorkflow(
      WorkflowConfiguration workflowConfiguration,
      DwcTerm coreDwcTerm,
      String downloadKey,
      Supplier<QueryExecutor> queryExecutorSupplier) {
    this.workflowConfiguration = workflowConfiguration;
    this.coreDwcTerm = coreDwcTerm;
    downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    download = downloadService.get(downloadKey);
    conceptClient = DownloadWorkflowModule.conceptClient(workflowConfiguration);
    translateVocabs(download);
    this.sqlDownloadRunner =
        SqlDownloadRunner.builder()
            .workflowConfiguration(workflowConfiguration)
            .download(download)
            .jobConfiguration(
                DownloadJobConfiguration.forSqlDownload(
                    download, workflowConfiguration.getHiveDBPath()))
            .queryExecutorSupplier(queryExecutorSupplier)
            .build();
  }

  public void run() {
    if (download.getRequest().getFormat() != DownloadFormat.SPECIES_LIST) {
      long recordCount = recordCount(download);
      if (isSmallDownloadCount(recordCount)) {
        runFromElastic();
        updateTotalRecordsCount(download, recordCount);
      } else {
        sqlDownloadRunner.run();
      }
    } else {
      sqlDownloadRunner.run();
    }
  }

  @SneakyThrows
  private void runFromElastic() {
    Properties settings = workflowConfiguration.getDownloadSettings();
    settings.setProperty(
        DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY,
        download.getRequest().getFormat().toString());
    WorkflowConfiguration configuration = new WorkflowConfiguration(settings);
    FromSearchDownloadAction.run(
        configuration,
        DownloadJobConfiguration.builder()
            .searchQuery(
                EsPredicateUtil.searchQuery(
                        ((PredicateDownloadRequest) download.getRequest()).getPredicate(),
                        DownloadWorkflowModule.esFieldMapper(
                            configuration.getEsIndexType()))
                    .toString())
            .downloadKey(download.getKey())
            .downloadTableName(DownloadUtils.downloadTableName(download.getKey()))
            .sourceDir(configuration.getTempDir())
            .isSmallDownload(true)
            .downloadFormat(configuration.getDownloadFormat())
            .coreTerm(coreDwcTerm)
            .extensions(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
            .build());
  }

  /** Updates the record count of the download entity. */
  private void updateTotalRecordsCount(Download download, long recordCount) {
    try {
      if (recordCount != ERROR_COUNT) {
        log.info("Updating record count({}) of download {}", recordCount, download);
        download.setTotalRecords(recordCount);
        downloadService.update(download);
      }
    } catch (Exception ex) {
      log.error(
          "Error updating record count for download workflow , reported count is {}",
          recordCount,
          ex);
    }
  }

  /** Method that determines if the search query produces a "small" download file. */
  private boolean isSmallDownloadCount(long recordCount) {
    return isSmallDownloadCount(recordCount, workflowConfiguration);
  }

  public static boolean isSmallDownloadCount(
      long recordCount, WorkflowConfiguration workflowConfiguration) {
    return recordCount != ERROR_COUNT
        && recordCount
            <= workflowConfiguration.getIntSetting(
                DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY);
  }

  private long recordCount(Download download) {

    try (DownloadEsClient downloadEsClient = downloadEsClient(workflowConfiguration)) {
      return downloadEsClient.getRecordCount(
          ((PredicateDownloadRequest) download.getRequest()).getPredicate());
    } catch (Exception ex) {
      return ERROR_COUNT;
    }
  }

  private DownloadEsClient downloadEsClient(WorkflowConfiguration workflowConfiguration) {
    return DownloadEsClient.builder()
        .esClient(DownloadWorkflowModule.esClient(workflowConfiguration))
        .esIndex(
            workflowConfiguration.getSetting(DownloadWorkflowModule.DefaultSettings.ES_INDEX_KEY))
        .esFieldMapper(DownloadWorkflowModule.esFieldMapper(workflowConfiguration.getEsIndexType()))
        .build();
  }

  private void translateVocabs(Download download) {
    if (download.getRequest() instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest =
          (PredicateDownloadRequest) download.getRequest();
      Predicate translatedPredicate =
          VocabularyFieldTranslator.translateVocabs(
              predicateDownloadRequest.getPredicate(), conceptClient);
      predicateDownloadRequest.setPredicate(translatedPredicate);
    }
  }
}
