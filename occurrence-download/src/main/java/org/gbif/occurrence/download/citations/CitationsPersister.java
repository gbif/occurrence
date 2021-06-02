package org.gbif.occurrence.download.citations;

import com.google.common.base.Preconditions;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Persists a download's citations to the Registry.
 */
public final class CitationsPersister extends CitationsFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CitationsPersister.class);

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    readCitationsAndUpdateLicense(
      properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY),
      Preconditions.checkNotNull(args[0]),
      new PersistUsage(
        Preconditions.checkNotNull(args[1]),
        properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY)
      ));
  }

  /**
   * Private constructor.
   */
  private CitationsPersister() {
    //empty constructor
  }

  /**
   * Persists the dataset usage and license info into the Registry data base.
   */
  public static class PersistUsage implements BiConsumer<Map<UUID,Long>,Map<UUID,License>> {

    private final String downloadKey;
    private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);
    private final OccurrenceDownloadService downloadService;

    public PersistUsage(String downloadKey, String registryWsUrl) {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil(registryWsUrl);
      this.downloadKey = downloadKey;
      this.downloadService = registryClientUtil.setupOccurrenceDownloadService();
    }

    @Override
    public void accept(Map<UUID, Long> datasetsCitation, Map<UUID, License> datasetLicenses) {
      if (datasetsCitation == null || datasetsCitation.isEmpty()) {
        LOG.info("No citation information to update as list of datasets is empty or null, hence ignoring the request");
      }

      try {
        datasetLicenses.values().forEach(licenseSelector::collectLicense);
        Long totalRecords = datasetsCitation.values().stream().reduce(0L, Long::sum);
        Download download = downloadService.get(downloadKey);
        download.setLicense(licenseSelector.getSelectedLicense());
        download.setTotalRecords(totalRecords);
        downloadService.update(download);
      } catch (Exception ex) {
        LOG.error("Error persisting download license information, downloadKey: {}, licenses: {} ",
          downloadKey, datasetLicenses.values(), ex);
      }

      try {
        downloadService.createUsages(downloadKey, datasetsCitation);
      } catch (Exception e) {
        LOG.error("Error persisting dataset usage information: {}", datasetsCitation, e);
      }
    }
  }
}
