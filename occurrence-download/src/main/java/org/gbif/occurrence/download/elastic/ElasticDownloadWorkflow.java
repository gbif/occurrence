package org.gbif.occurrence.download.elastic;

import static org.gbif.occurrence.download.util.VocabularyUtils.translateVocabs;

import java.util.Properties;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.action.FromSearchDownloadAction;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.util.DownloadRequestUtils;
import org.gbif.occurrence.search.es.EsPredicateUtil;
import org.gbif.vocabulary.client.ConceptClient;

@Slf4j
public class ElasticDownloadWorkflow {

  // arbitrary record count that represents and error counting the records of the input query
  private static final int ERROR_COUNT = -1;
  private static final int ES_COUNT_MARGIN_ERROR = 5000;

  private final OccurrenceDownloadService downloadService;

  private final DwcTerm coreDwcTerm;

  private final WorkflowConfiguration workflowConfiguration;

  private final Download download;

  @Builder
  public ElasticDownloadWorkflow(
      WorkflowConfiguration workflowConfiguration, DwcTerm coreDwcTerm, String downloadKey) {
    this.workflowConfiguration = workflowConfiguration;
    this.coreDwcTerm = coreDwcTerm;
    downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    download = downloadService.get(downloadKey);
    ConceptClient conceptClient = DownloadWorkflowModule.conceptClient(workflowConfiguration);
    translateVocabs(download, conceptClient);
  }

  @SneakyThrows
  public void run() {
    // check if meets ES download requirements
    if (download.getRequest().getFormat() != DownloadFormat.DWCA
        && download.getRequest().getFormat() != DownloadFormat.SIMPLE_CSV) {
      throw new IllegalArgumentException("Only dwca and simple csv downloads can be run in ES");
    }

    long recordCount = recordCount(download);
    if (!isSmallDownloadCount(recordCount)) {
      throw new IllegalArgumentException(
          "Download too big for ES. Number of records: " + recordCount);
    }

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
                          configuration.getEsIndexType(),
                          configuration.getDefaultChecklistKey()
                        ))
                    .toString())
            .downloadKey(download.getKey())
            .downloadTableName(DownloadUtils.downloadTableName(download.getKey()))
            .sourceDir(configuration.getTempDir())
            .isSmallDownload(true)
            .downloadFormat(configuration.getDownloadFormat())
            .coreTerm(coreDwcTerm)
            .extensions(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
            .build());

    updateTotalRecordsCount(download, recordCount);
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
                    DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY)
                + ES_COUNT_MARGIN_ERROR;
  }

  private long recordCount(Download download) {

    try (DownloadEsClient downloadEsClient = downloadEsClient(workflowConfiguration)) {
      return downloadEsClient.getRecordCount(
          ((PredicateDownloadRequest) download.getRequest()).getPredicate());
    } catch (Exception ex) {
      log.error("Error when getting download record count from ES", ex);
      return ERROR_COUNT;
    }
  }

  private DownloadEsClient downloadEsClient(WorkflowConfiguration workflowConfiguration) {
    return DownloadEsClient.builder()
        .esClient(DownloadWorkflowModule.esClient(workflowConfiguration))
        .esIndex(
            workflowConfiguration.getSetting(DownloadWorkflowModule.DefaultSettings.ES_INDEX_KEY))
        .esFieldMapper(DownloadWorkflowModule.esFieldMapper(
          workflowConfiguration.getEsIndexType(),
          workflowConfiguration.getDefaultChecklistKey()
        ))
        .build();
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
}
