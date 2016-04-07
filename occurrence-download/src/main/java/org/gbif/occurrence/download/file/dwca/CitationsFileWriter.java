package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

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
   * @param datasetOccUsageService usage service
   * @param downloadKey            download key
   */
  public static void createCitationFile(Map<UUID, Long> datasetUsages, String citationFileName,
                                        DatasetOccurrenceDownloadUsageService datasetOccUsageService,
                                        DatasetService datasetService, String downloadKey) {
    if (datasetUsages != null && !datasetUsages.isEmpty()) {
      try (ICsvBeanWriter beanWriter = new CsvBeanWriter(new FileWriterWithEncoding(citationFileName, Charsets.UTF_8),
                                                         CsvPreference.TAB_PREFERENCE)) {
        for (Entry<UUID, Long> entry : datasetUsages.entrySet()) {
          if (entry.getKey() != null) {
            beanWriter.write(new Facet.Count(entry.getKey().toString(), entry.getValue()), HEADER, PROCESSORS);
            persistDatasetUsage(entry, downloadKey, datasetOccUsageService, datasetService);
          }
        }
        beanWriter.flush();
      } catch (IOException e) {
        LOG.error("Error creating citations file", e);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Persists the dataset usage information and swallows any exception to avoid an error during the file building.
   */
  private static void persistDatasetUsage(Entry<UUID, Long> usage, String downloadKey,
                                          DatasetOccurrenceDownloadUsageService datasetOccUsageService,
                                          DatasetService datasetService) {
    try {
      Dataset dataset = datasetService.get(usage.getKey());
      if (dataset != null) { //the dataset still exists
        DatasetOccurrenceDownloadUsage datasetUsage = new DatasetOccurrenceDownloadUsage();
        datasetUsage.setDatasetKey(dataset.getKey());
        datasetUsage.setNumberRecords(usage.getValue());
        datasetUsage.setDownloadKey(downloadKey);
        datasetUsage.setDatasetDOI(dataset.getDoi());
        if (dataset.getCitation() != null && dataset.getCitation().getText() != null) {
          datasetUsage.setDatasetCitation(dataset.getCitation().getText());
        }
        datasetUsage.setDatasetTitle(dataset.getTitle());
        datasetOccUsageService.create(datasetUsage);
      }
    } catch (Exception e) {
      LOG.error("Error persisting dataset usage information", e);
    }
  }

  /**
   * Private/default constructor.
   */
  private CitationsFileWriter() {
    // private constructor
  }

}
