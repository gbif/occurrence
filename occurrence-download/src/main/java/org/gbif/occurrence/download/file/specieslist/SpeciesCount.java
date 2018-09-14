package org.gbif.occurrence.download.file.specieslist;

import java.io.IOException;
import java.util.Properties;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

public class SpeciesCount {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesCount.class);

  public static void main(String[] args) throws IOException {
    String countPath = Preconditions.checkNotNull(args[0]);
    String downloadKey = Preconditions.checkNotNull(args[1]);
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    String nameNode = properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY);
    String registryWsURL = properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY);
    persistCount(downloadKey, DownloadFileUtils.readSpeciesCount(nameNode, countPath),registryWsURL);
  }

  /**
   * Updates the species record count of the download entity.
   */
  static void persistCount(String downloadKey, long recordCount, String registryWsURL) {
    try {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil();
      OccurrenceDownloadService occurrenceDownloadService =
          registryClientUtil.setupOccurrenceDownloadService(registryWsURL);
      LOG.info("Updating record count({}) of download {}", recordCount, downloadKey);
      Download download = occurrenceDownloadService.get(downloadKey);
      if (download == null) {
        LOG.error("Download {} was not found!", downloadKey);
      } else {
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      LOG.error("Error updating record count for download workflow {}, reported count is {}",
          downloadKey, recordCount, ex);
    }
  }
}
