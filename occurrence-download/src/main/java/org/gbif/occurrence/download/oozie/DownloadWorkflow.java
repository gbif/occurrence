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
package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadWorkflow {

  // arbitrary record count that represents and error counting the records of the input query
  private static final int ERROR_COUNT = -1;

  private final OccurrenceDownloadService downloadService;

  private final DwcTerm coreDwcTerm;

  private final WorkflowConfiguration workflowConfiguration;

  private final Download download;

  public static void main(String[] args) {

    DownloadWorkflow.builder()
      .downloadKey(args[0])
      .coreDwcTerm(DwcTerm.valueOf(args[1]))
      .workflowConfiguration(new WorkflowConfiguration())
      .build()
      .run();
  }


  @Builder
  public DownloadWorkflow(WorkflowConfiguration workflowConfiguration, DwcTerm coreDwcTerm, String downloadKey) {
    this.workflowConfiguration = workflowConfiguration;
    this.coreDwcTerm = coreDwcTerm;
    downloadService = DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    download = downloadService.get(downloadKey);
  }

  public void run() {

    if (download.getRequest().getFormat() != DownloadFormat.SPECIES_LIST) {
      long recordCount = recordCount(download);
      updateTotalRecordsCount(download, recordCount);
      if (isSmallDownloadCount(recordCount)) {
        //runFromElastic()
      } else {
        //runFromHive()
      }
    } else {
      //runFromHive();
    }

  }


  /**
   * Updates the record count of the download entity.
   */
  private void updateTotalRecordsCount(Download download, long recordCount) {
    try {
      if (recordCount != ERROR_COUNT) {
        log.info("Updating record count({}) of download {}", recordCount, download);
        download.setTotalRecords(recordCount);
        downloadService.update(download);
      }
    } catch (Exception ex) {
      log.error("Error updating record count for download workflow , reported count is {}", recordCount, ex);
    }
  }

  /**
   * Method that determines if the search query produces a "small" download file.
   */
  private Boolean isSmallDownloadCount(long recordCount) {
    return recordCount != ERROR_COUNT && recordCount <= workflowConfiguration.getIntSetting(DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY);
  }

  private long recordCount(Download download) {

    try (DownloadEsClient downloadEsClient = downloadEsClient(workflowConfiguration)) {
       return downloadEsClient.getRecordCount(((PredicateDownloadRequest)download.getRequest()).getPredicate());
    } catch (Exception ex) {
      return ERROR_COUNT;
    }
  }


  private DownloadEsClient downloadEsClient(WorkflowConfiguration workflowConfiguration) {
    return DownloadEsClient.builder()
            .esClient(DownloadWorkflowModule.esClient(workflowConfiguration))
            .esIndex(workflowConfiguration.getSetting(DownloadWorkflowModule.DefaultSettings.ES_INDEX_KEY))
            .esFieldMapper(DownloadWorkflowModule.esFieldMapper(workflowConfiguration.getEsIndexType()))
            .build();
  }

}
