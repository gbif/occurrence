package org.gbif.occurrence.download.file.dwca;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;

/**
 * Utility class that creates a datset citations file from Map that contains the dataset usages (record count).
 * The output file contains a list of datasets keys/uuids and its counts of occurrence records.
 */
public final class CitationsFileWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CitationsFileWriter.class);

  // Java fields of class solr.FacetField.Count that are used to create the citations file.
  private static final String[] HEADER = {"name", "count"};

  // Processors used to create the citations file.
  private static final CellProcessor[] PROCESSORS = {new NotNull(), new ParseLong()};

  /**
   * Creates the dataset citation file using the the Solr query response.
   *
   * @param datasetUsages          record count per dataset
   * @param citationFileName       output file name
   * @param occDownloadService     occurrence downlaod service
   * @param downloadKey            download key
   */
  public static void createCitationFile(Map<UUID, Long> datasetUsages, String citationFileName,
                                        OccurrenceDownloadService occDownloadService, String downloadKey) {
    if (datasetUsages != null && !datasetUsages.isEmpty()) {
      try (ICsvBeanWriter beanWriter = new CsvBeanWriter(new FileWriterWithEncoding(citationFileName, Charsets.UTF_8),
                                                         CsvPreference.TAB_PREFERENCE)) {
        for (Entry<UUID, Long> entry : datasetUsages.entrySet()) {
          if (entry.getKey() != null) {
            beanWriter.write(new Facet.Count(entry.getKey().toString(), entry.getValue()), HEADER, PROCESSORS);
          }
        }
        beanWriter.flush();
        persistUsages(occDownloadService, downloadKey, datasetUsages);
      } catch (IOException e) {
        LOG.error("Error creating citations file", e);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Persist dataset usages and swallow the any exception.
   */
  private static void persistUsages(OccurrenceDownloadService occDownloadService, String downloadKey, Map<UUID, Long> datasetUsages) {
    try {
      occDownloadService.createUsages(downloadKey, datasetUsages);
    } catch (Exception ex) {
      LOG.error("Error persisting usages for download {}", downloadKey, ex);
    }
  }

  /**
   * Private/default constructor.
   */
  private CitationsFileWriter() {
    // private constructor
  }

}
