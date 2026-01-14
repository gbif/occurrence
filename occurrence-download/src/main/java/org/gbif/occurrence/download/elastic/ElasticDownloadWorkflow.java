package org.gbif.occurrence.download.elastic;

import static org.gbif.occurrence.download.util.VocabularyUtils.translateOccurrencePredicateFields;

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
import org.gbif.search.es.occurrence.OccurrenceEsField;
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
    this.downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    this.download = downloadService.get(downloadKey);
    ConceptClient conceptClient = DownloadWorkflowModule.conceptClient(workflowConfiguration);
    translateOccurrencePredicateFields(download, conceptClient);
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
    log.info("Download with checklistKey {}", download.getRequest().getChecklistKey());
    FromSearchDownloadAction.run(
        configuration,
        DownloadJobConfiguration.builder()
            .searchQuery(
                EsPredicateUtil.searchQuery(
                        ((PredicateDownloadRequest) download.getRequest()).getPredicate(),
                        OccurrenceEsField.buildFieldMapper(),
                        workflowConfiguration.getDefaultChecklistKey())
                    .toString())
            .checklistKey(
                download.getRequest().getChecklistKey() != null
                    ? download.getRequest().getChecklistKey()
                    : configuration.getDefaultChecklistKey())
            .downloadKey(download.getKey())
            .downloadTableName(DownloadUtils.downloadTableName(download.getKey()))
            .sourceDir(configuration.getTempDir())
            .isSmallDownload(true)
            .downloadFormat(configuration.getDownloadFormat())
            .coreTerm(coreDwcTerm)
            .verbatimExtensions(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
            .build());

    updateTotalRecordsCount(download.getKey(), recordCount);
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
    // if set, dont recalculate
    if (download.getTotalRecords() > 0){
      return download.getTotalRecords();
    }

    log.info("Download records count: {}, re-querying ES for accurate count", download.getTotalRecords());
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
        .esFieldMapper(OccurrenceEsField.buildFieldMapper())
        .build();
  }

  /** Updates the record count of the download entity. */
  private void updateTotalRecordsCount(String downloadKey, long recordCount) {
    try {
      if (recordCount != ERROR_COUNT) {
        // it's important to get the up-to-date download since it's modified in other parts of the wf
        Download download = downloadService.get(downloadKey);
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
