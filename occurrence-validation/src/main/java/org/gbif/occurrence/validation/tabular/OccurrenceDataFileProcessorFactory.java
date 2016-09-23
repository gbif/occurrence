package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.tabular.parallel.ParallelDataFileProcessor;
import org.gbif.occurrence.validation.tabular.processor.OccurrenceLineProcessorFactory;
import org.gbif.occurrence.validation.tabular.single.SingleDataFileProcessor;

public class OccurrenceDataFileProcessorFactory {

  public static final int FILE_SPLIT_SIZE = 10000;

  private final String apiUrl;


  public OccurrenceDataFileProcessorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }


  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public DataFileProcessor create(DataFile dataFile) {
    OccurrenceLineProcessorFactory factory = new OccurrenceLineProcessorFactory(apiUrl);

    if (dataFile.getNumOfLines() <= FILE_SPLIT_SIZE) {
       return new SingleDataFileProcessor(factory.create());
    }
    return new ParallelDataFileProcessor(apiUrl);
  }


}
