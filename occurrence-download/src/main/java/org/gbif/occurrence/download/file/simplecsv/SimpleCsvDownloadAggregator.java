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
package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.utils.file.FileUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combine the parts created by actor and combine them into single zip file.
 */
public class SimpleCsvDownloadAggregator implements DownloadAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvDownloadAggregator.class);

  private static final String CSV_EXTENSION = ".csv";

  private final DownloadJobConfiguration configuration;
  private final WorkflowConfiguration workflowConfiguration;
  private final String outputFileName;

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);
  @Inject
  public SimpleCsvDownloadAggregator(DownloadJobConfiguration configuration,
                                     WorkflowConfiguration workflowConfiguration,
                                     OccurrenceDownloadService occurrenceDownloadService) {
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
    outputFileName =
      configuration.getDownloadTempDir() + Path.SEPARATOR + configuration.getDownloadKey() + CSV_EXTENSION;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    try {
      if (!results.isEmpty()) {
        mergeResults(results);
      }
      SimpleCsvArchiveBuilder.withHeader(DownloadTerms.SIMPLE_DOWNLOAD_TERMS)
                             .mergeToZip(FileSystem.getLocal(new Configuration()).getRawFileSystem(),
                                         DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode()),
                                         configuration.getDownloadTempDir(),
                                         workflowConfiguration.getHdfsOutputPath(),
                                         configuration.getDownloadKey(),
                                         ModalZipOutputStream.MODE.DEFAULT);
      //Delete the temp directory
      FileUtils.deleteDirectoryRecursively(Paths.get(configuration.getDownloadTempDir()).toFile());
    } catch (IOException ex) {
      LOG.error("Error aggregating download files", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Merges the files of each job into a single CSV file.
   */
  private void mergeResults(List<Result> results) {
    try (FileOutputStream outputFileWriter = new FileOutputStream(outputFileName, true)) {
      // Results are sorted to respect the original ordering
      Collections.sort(results);
      DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
      for (Result result : results) {
        datasetUsagesCollector.sumUsages(result.getDatasetUsages());
        datasetUsagesCollector.mergeLicenses(result.getDatasetLicenses());
        DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName(), outputFileWriter);
      }
      LOG.debug("Create usage for download key: {}", configuration.getDownloadKey());
      occurrenceDownloadService.createUsages(configuration.getDownloadKey(), datasetUsagesCollector.getDatasetUsages());
      persistDownloadLicense(configuration.getDownloadKey(), datasetUsagesCollector.getDatasetLicenses());
    } catch (Exception ex) {
      LOG.error("Error merging results", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Persist download license that was assigned to the occurrence download.
   */
  private void persistDownloadLicense(String downloadKey, Set<License> licenses) {
    try {
      licenses.forEach(licenseSelector::collectLicense);
      Download download = occurrenceDownloadService.get(configuration.getDownloadKey());
      download.setLicense(licenseSelector.getSelectedLicense());
      occurrenceDownloadService.update(download);
    } catch (Exception ex) {
      LOG.error("Error persisting download license information, downloadKey: {}, licenses:{} ", downloadKey, licenses,
                ex);
    }
  }
}
