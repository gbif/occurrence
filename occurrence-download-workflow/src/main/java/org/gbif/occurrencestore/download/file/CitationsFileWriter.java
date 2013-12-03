package org.gbif.occurrencestore.download.file;

import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Utility class that creates the citations file using a faceted Solr response.
 * The output file contains a list of datasets keys/uuids and its counts of occurrence records.
 */
public class CitationsFileWriter {

  // Java fields of class solr.FacetField.Count that are used to create the citations file.
  private static final String[] HEADER = new String[] {"name", "count"};

  // Processors for fields name and count of class solr.FacetField.Count
  private static final CellProcessor[] PROCESSORS = new CellProcessor[] {new NotNull(), new ParseLong()};

  /**
   * Private/default constructor.
   */
  private CitationsFileWriter() {
    // private constructor
  }

  /**
   * Creates the dataset citation file using the the Solr query response.
   * 
   * @param datasetUsages record count per dataset
   * @param citationFileName output file name
   * @param datasetOccUsageService usage service
   * @param downloadKey download key
   * @throws IOException if an error occurs while the citation file is being created
   */
  public static void createCitationFile(final Map<UUID, Long> datasetUsages, String citationFileName,
    DatasetOccurrenceDownloadUsageService datasetOccUsageService, String downloadKey) throws IOException {
    if (datasetUsages != null && !datasetUsages.isEmpty()) {
      ICsvBeanWriter beanWriter = null;
      Closer closer = Closer.create();
      try {
        beanWriter =
          new CsvBeanWriter(new FileWriterWithEncoding(citationFileName, Charsets.UTF_8), CsvPreference.TAB_PREFERENCE);
        closer.register(beanWriter);
        for (Entry<UUID, Long> entry : datasetUsages.entrySet()) {
          if (entry.getKey() != null) {
            beanWriter.write(new Facet.Count(entry.getKey().toString(), entry.getValue()), HEADER, PROCESSORS);
            persistDatasetUsage(entry, downloadKey, datasetOccUsageService);
          }
        }
      } catch (IOException e) {
        closer.rethrow(e);
      } finally {
        if (beanWriter != null) {
          beanWriter.flush();
        }
        closer.close();
      }
    }
  }

  /**
   * Persists the dataset usage information and swallows any exception to avoid an error during the file building.
   */
  private static void persistDatasetUsage(Entry<UUID, Long> usage, String downloadKey,
    DatasetOccurrenceDownloadUsageService datasetOccUsageService) {
    try {
      DatasetOccurrenceDownloadUsage datasetUsage = new DatasetOccurrenceDownloadUsage();
      datasetUsage.setDatasetKey(usage.getKey());
      datasetUsage.setNumberRecords(usage.getValue());
      datasetUsage.setDownloadKey(downloadKey);
      datasetOccUsageService.create(datasetUsage);
    } catch (Exception e) {
      System.err.println("Error persisting dataset usage information");
      e.printStackTrace();
    }
  }

}
