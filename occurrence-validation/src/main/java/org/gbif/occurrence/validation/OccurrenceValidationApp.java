package org.gbif.occurrence.validation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.tabular.ParallelDataFileProcessor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class OccurrenceValidationApp {

  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    DataFile dataFile = new DataFile();
    dataFile.setFileName(fileName);
    dataFile.setNumOfLines(FileBashUtilities.countLines(fileName));
    dataFile.setDelimiterChar('\t');
    dataFile.setHasHeaders(true);
    dataFile.loadHeaders();
    ParallelDataFileProcessor parallelDataFileProcessor = new ParallelDataFileProcessor(args[1]);
    Map<OccurrenceIssue, LongAdder> result = parallelDataFileProcessor.process(dataFile);
    System.out.println(result.toString());
  }
}
