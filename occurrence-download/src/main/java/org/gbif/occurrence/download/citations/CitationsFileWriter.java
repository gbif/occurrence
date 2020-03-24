package org.gbif.occurrence.download.citations;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.common.download.DownloadException;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.FileUtils;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Writes a dataset's citations to a text file adjacent to the download file.
 */
public final class CitationsFileWriter extends CitationsFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CitationsFileWriter.class);

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    readCitationsAndUpdateLicense(
      properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY),
      Preconditions.checkNotNull(args[0]),
      new WriteCitations(
        new WorkflowConfiguration(),
        Preconditions.checkNotNull(args[1]),
        properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY)
      ));
  }

  /**
   * Private constructor.
   */
  private CitationsFileWriter() {
    //empty constructor
  }

  public static class WriteCitations implements BiConsumer<Map<UUID,Long>,Map<UUID,License>> {
    private static final String DATASET_TITLE_FMT = "GBIF Occurrence Download %s\n";

    private final String downloadKey;
    private final OccurrenceDownloadService downloadService;
    private final DatasetService datasetService;

    private final WorkflowConfiguration workflowConfiguration;

    public WriteCitations(WorkflowConfiguration workflowConfiguration, String downloadKey, String registryWsUrl) {
      this.workflowConfiguration = workflowConfiguration;
      RegistryClientUtil registryClientUtil = new RegistryClientUtil();
      this.downloadKey = downloadKey;
      this.downloadService = registryClientUtil.setupOccurrenceDownloadService(registryWsUrl);
      this.datasetService = registryClientUtil.setupDatasetService(registryWsUrl);
    }

    @Override
    public void accept(Map<UUID, Long> datasetsCitation, Map<UUID, License> datasetLicenses) {
      try {
        FileSystem targetFs = FileSystem.get(workflowConfiguration.getHadoopConf());

        String tmpDir = workflowConfiguration.getTempDir();
        File archiveDir = new File(tmpDir, downloadKey);
        if (archiveDir.exists()) {
          LOG.info("Cleaning up existing archive directory {}", archiveDir.getPath());
          FileUtils.deleteDirectoryRecursively(archiveDir);
        }
        archiveDir.mkdirs();

        File citationsTempFile = new File(tmpDir, downloadKey + ".citation");
        LOG.info("Start writing the citations file into {}", citationsTempFile);
        try (Writer citationWriter = FileUtils.startNewUtf8File(citationsTempFile)) {
          Download download = downloadService.get(downloadKey);
          String downloadDoi = download.getDoi().getUrl().toString();

          // write the GBIF download citation
          Citation citation = new Citation(String.format(DATASET_TITLE_FMT, downloadDoi), downloadDoi);
          citationWriter.write(citation.getText());

          // now iterate over constituent UUIDs
          for (Map.Entry<UUID, Long> constituent : datasetsCitation.entrySet()) {
            LOG.info("Processing constituent dataset: {}", constituent.getKey());
            // catch errors for each UUID to make sure one broken dataset does not bring down the entire process
            try {
              Dataset dataset = datasetService.get(constituent.getKey());

              // citation
              writeCitation(citationWriter, dataset);
            } catch (UniformInterfaceException e) {
              LOG.error("Registry client http exception: {} \n {}", e.getResponse().getStatus(),
                e.getResponse().getEntity(String.class), e);
            } catch (Exception e) {
              LOG.error("Error creating download file", e);
            }
          }
        }

        targetFs.moveFromLocalFile(
          new Path(citationsTempFile.getPath()),
          new Path(workflowConfiguration.getHdfsOutputPath(),
            downloadKey + ".txt")
        );

      } catch (IOException e) {
        throw new DownloadException(e);
      }
    }

    private static void writeCitation(Writer citationWriter, Dataset dataset)
      throws IOException {
      // citation
      if (dataset.getCitation() != null && !Strings.isNullOrEmpty(dataset.getCitation().getText())) {
        citationWriter.write(dataset.getCitation().getText());
        citationWriter.write('\n');
      } else {
        LOG.error("Constituent dataset misses mandatory citation for id: {}", dataset.getKey());
      }
    }
  }
}
