package org.gbif.occurrence.validation;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.tabular.OccurrenceDataFileProcessorFactory;
import org.gbif.occurrence.validation.util.FileBashUtilities;

import java.io.IOException;

public class OccurrenceValidationApp {

  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    DataFile dataFile = new DataFile();
    dataFile.setFileName(fileName);
    dataFile.setNumOfLines(FileBashUtilities.countLines(fileName));
    dataFile.setDelimiterChar('\t');
    dataFile.setHasHeaders(true);
    dataFile.loadHeaders();
    OccurrenceDataFileProcessorFactory dataFileProcessorFactory = new OccurrenceDataFileProcessorFactory(args[1]);
    DataFileProcessor dataFileProcessor = dataFileProcessorFactory.create(dataFile);
    DataFileValidationResult result = dataFileProcessor.process(dataFile);
    System.out.println(result.toString());
  }
}
