package org.gbif.occurrence.download.file.specieslist;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Oozie action for Species list download, helps with counts of the number of distinct species.
 *
 */
public class SpeciesCount {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesCount.class);

  private SpeciesCount() {}

  /**
   * Updates the species record count of the download entity.
   */
  static void persist(String downloadKey, long recordCount, OccurrenceDownloadService occurrenceDownloadService) {
    try {

      LOG.info("Updating record count({}) of download {}", recordCount, downloadKey);
      Download download = occurrenceDownloadService.get(downloadKey);
      if (download == null) {
        LOG.error("Download {} was not found!", downloadKey);
      } else {
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      LOG.error("Error updating record count for download workflow {}, reported count is {}", downloadKey, recordCount, ex);
    }
  }
}
