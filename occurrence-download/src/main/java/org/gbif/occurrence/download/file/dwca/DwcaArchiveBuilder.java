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
package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.dwca.archive.CitationFileReader;
import org.gbif.occurrence.download.file.dwca.archive.ConstituentsDatasetsProcessor;
import org.gbif.occurrence.download.file.dwca.archive.DownloadArchiveBuilder;
import org.gbif.occurrence.download.file.dwca.archive.DownloadMetadataBuilder;
import org.gbif.occurrence.download.file.dwca.archive.DownloadUsagesPersist;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.occurrence.query.TitleLookupServiceFactory;

import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.fs.FileSystem;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.util.ArchiveFileUtils.initializeArchiveDir;

/**
 * Creates a DWC archive for occurrence downloads based on the hive query result files generated
 * during the Oozie workflow. It creates a local archive folder with an occurrence data file and a dataset sub-folder
 * that contains an EML metadata file per dataset involved.
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaArchiveBuilder {

  private final DownloadJobConfiguration jobConfiguration;

  private final WorkflowConfiguration workflowConfiguration;

  //Caches
  private final RegistryClientUtil registryClientUtil;
  private final FileSystem sourceFs;
  private final FileSystem targetFs;
  private final File archiveDir;
  private final Download download;
  private final DownloadMetadataBuilder metadataBuilder;
  private final ConstituentsDatasetsProcessor constituentsDatasetsProcessor;
  private final DownloadUsagesPersist downloadUsagesPersist;
  private final OccurrenceDownloadService occurrenceDownloadService;
  private final DatasetService datasetService;


  public DwcaArchiveBuilder(DownloadJobConfiguration jobConfiguration, WorkflowConfiguration workflowConfiguration) {
    //Configs
    this.jobConfiguration = jobConfiguration;
    this.workflowConfiguration = workflowConfiguration;

    //Ws clients and client utils
    this.registryClientUtil = new RegistryClientUtil(workflowConfiguration.getRegistryWsUrl());
    occurrenceDownloadService = getOccurrenceDownloadService();
    datasetService = getDatasetService();
    download = getDownload();
    downloadUsagesPersist = getDownloadUsagesPersist();

    //File systems
    sourceFs = getSourceFs();
    targetFs = getTargetFs();
    archiveDir = getArchiveDir();

    initializeArchiveDir(archiveDir, jobConfiguration);

    //Archive helpers
    metadataBuilder = getDownloadMetadataBuilder();
    constituentsDatasetsProcessor = getConstituentsDatasetsProcessor();
  }

  public static DwcaArchiveBuilder of(DownloadJobConfiguration configuration) {
    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();
    return new DwcaArchiveBuilder(configuration, workflowConfiguration);
  }

  public static DwcaArchiveBuilder of(DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration) {
    return new DwcaArchiveBuilder(configuration, workflowConfiguration);
  }

  @SneakyThrows
  private FileSystem getSourceFs() {
    return jobConfiguration.isSmallDownload()? FileSystem.getLocal(workflowConfiguration.getHadoopConf()) : FileSystem.get(workflowConfiguration.getHadoopConf());
  }

  @SneakyThrows
  private FileSystem getTargetFs() {
    return FileSystem.get(workflowConfiguration.getHadoopConf());
  }

  @SneakyThrows
  private File getArchiveDir() {
    return new File(workflowConfiguration.getTempDir(), jobConfiguration.getDownloadKey());
  }

  private Download getDownload() {
    return occurrenceDownloadService.get(jobConfiguration.getDownloadKey());
  }

  private DatasetService getDatasetService(){
    return registryClientUtil.datasetService();
  }

  private OccurrenceDownloadService getOccurrenceDownloadService(){
    return registryClientUtil.occurrenceDownloadService(jobConfiguration.getCoreTerm());
  }

  @SneakyThrows
  private URI getDownloadLink(Download download) {
    return new URI(workflowConfiguration.getDownloadLink(download.getKey()));
  }

  private DownloadMetadataBuilder getDownloadMetadataBuilder() {
    return DownloadMetadataBuilder.builder()
            .downloadLinkProvider(this::getDownloadLink)
            .archiveDir(archiveDir)
            .titleLookup(TitleLookupServiceFactory.getInstance(workflowConfiguration.getApiUrl()))
            .download(download)
            .build();
  }

  private ConstituentsDatasetsProcessor getConstituentsDatasetsProcessor() {
    return ConstituentsDatasetsProcessor.builder()
            .datasetService(datasetService)
            .archiveDir(archiveDir)
            .build();
  }

  private DownloadUsagesPersist getDownloadUsagesPersist() {
    return DownloadUsagesPersist.create(occurrenceDownloadService);
  }

  private CitationFileReader citationFileReader() {
    return CitationFileReader.builder()
      .sourceFs(sourceFs)
      .citationFileName(jobConfiguration.getCitationDataFileName())
      .datasetService(datasetService)
      .onRead(constituentDataset -> {
        constituentsDatasetsProcessor.accept(constituentDataset);
        metadataBuilder.accept(constituentDataset);
      })
      .onFinish(datasetUsages -> {
        //Persist Dataset Usages
        downloadUsagesPersist.persistUsages(download.getKey(), datasetUsages);

        // persist the License assigned to the download
        downloadUsagesPersist.persistDownloadLicense(download, constituentsDatasetsProcessor.getSelectedLicense());

        // metadata about the entire archive data
        metadataBuilder.writeMetadata();
        closeSilently(constituentsDatasetsProcessor);
        deleteSilently(jobConfiguration.getCitationDataFileName());
      }).build();
  }

  @SneakyThrows
  private static void deleteSilently(String file) {
    Files.deleteIfExists(Paths.get(file));
  }

  @SneakyThrows
  private static void closeSilently(Closeable closeable) {
    closeable.close();
  }

  private DownloadArchiveBuilder getArchiveBuilder() {
    return DownloadArchiveBuilder.builder()
      .download(download)
      .archiveDir(archiveDir)
      .workflowConfiguration(workflowConfiguration)
      .configuration(jobConfiguration)
      .sourceFs(sourceFs)
      .targetFs(targetFs)
      .citationFileReader(citationFileReader())
      .build();
  }

  public void buildArchive() {
    // build archive
    DownloadArchiveBuilder archiveBuilder = getArchiveBuilder();
    archiveBuilder.buildArchive();
  }
}
