package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceDataFileProcessorFactory {


  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDataFileProcessorFactory.class);

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 50000L;

  private final String apiUrl;


  public OccurrenceDataFileProcessorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }



  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public DataFileProcessor create(DataFile dataFile) {
    return new ParallelDataFileProcessor(apiUrl);
  }


}
