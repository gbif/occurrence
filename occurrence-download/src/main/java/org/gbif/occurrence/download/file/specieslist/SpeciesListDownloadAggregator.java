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
package org.gbif.occurrence.download.file.specieslist;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadCount;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvArchiveBuilder;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.utils.file.FileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Throwables;

/**
 * Aggregates multiple files from different jobs and merge there result to final file.
 *
 */
public class SpeciesListDownloadAggregator implements DownloadAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesListDownloadAggregator.class);

  private static final String CSV_EXTENSION = ".csv";

  private static final String[] COLUMNS = DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.stream()
    .map(DownloadTerms::simpleName)
    .toArray(String[]::new);

  private final DownloadJobConfiguration configuration;
  private final WorkflowConfiguration workflowConfiguration;
  private final String outputFileName;

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);

  @Inject
  public SpeciesListDownloadAggregator(DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration,
      OccurrenceDownloadService occurrenceDownloadService) {
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
    outputFileName = configuration.getDownloadTempDir() + Path.SEPARATOR + configuration.getDownloadKey() + CSV_EXTENSION;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  @Override
  public void aggregate(List<Result> results) {
    try {
      if (!results.isEmpty()) {
        mergeResults(results);
      }
      SimpleCsvArchiveBuilder.withHeader(DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS).mergeToZip(
          FileSystem.getLocal(new Configuration()).getRawFileSystem(), DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode()),
          configuration.getDownloadTempDir(), workflowConfiguration.getHdfsOutputPath(), configuration.getDownloadKey(),
          ModalZipOutputStream.MODE.DEFAULT);
      // Delete the temp directory
      FileUtils.deleteDirectoryRecursively(Paths.get(configuration.getDownloadTempDir()).toFile());
    } catch (IOException ex) {
      LOG.error("Error aggregating download files", ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Merges the files of each job into a single CSV file.
   */
  private void mergeResults(List<Result> results) {
    // Results are sorted to respect the original ordering
    Collections.sort(results);
    DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
    SpeciesListCollector speciesListCollector = new SpeciesListCollector();

    for (Result result : results) {
      SpeciesListResult speciesResult = (SpeciesListResult) result;
      datasetUsagesCollector.sumUsages(speciesResult.getDatasetUsages());
      datasetUsagesCollector.mergeLicenses(speciesResult.getDatasetLicenses());
      speciesResult.getDistinctSpecies().iterator().forEachRemaining(speciesListCollector::collect);
    }
    exportToFile(outputFileName, speciesListCollector);
    occurrenceDownloadService.createUsages(configuration.getDownloadKey(), datasetUsagesCollector.getDatasetUsages());
    persistDownloadLicense(configuration.getDownloadKey(), datasetUsagesCollector.getDatasetLicenses());
    DownloadCount.persist(configuration.getDownloadKey(), speciesListCollector.getDistinctSpecies().size(), occurrenceDownloadService);
  }

  private void exportToFile(String outputFileName, SpeciesListCollector speciesListCollector) {
    try (ICsvMapWriter csvMapWriter =
        new CsvMapWriter(new FileWriterWithEncoding(outputFileName, StandardCharsets.UTF_8), CsvPreference.TAB_PREFERENCE)) {
      Set<Map<String, String>> distinctSpecies = speciesListCollector.getDistinctSpecies();
      distinctSpecies.iterator().forEachRemaining(speciesInfo -> {
        try {
          csvMapWriter.write(speciesInfo, COLUMNS);
        } catch (IOException e) {
          LOG.error("Error merging results", e);
          throw Throwables.propagate(e);
        }
      });
    } catch (Exception e) {
      LOG.error("Error merging results", e);
      throw Throwables.propagate(e);
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
      LOG.error("Error persisting download license information, downloadKey: {}, licenses:{} ", downloadKey, licenses, ex);
    }
  }

}
