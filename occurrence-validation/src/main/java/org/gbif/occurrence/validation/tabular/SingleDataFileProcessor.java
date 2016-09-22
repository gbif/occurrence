package org.gbif.occurrence.validation.tabular;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.RecordProcessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class SingleDataFileProcessor<T> implements DataFileProcessor<Map<OccurrenceIssue, LongAdder>> {

  private final RecordProcessor recordProcessor;

  private final SimpleValidationCollector collector = new SimpleValidationCollector();

  public SingleDataFileProcessor(RecordProcessor recordProcessor) {
    this.recordProcessor = recordProcessor;
  }

  @Override
  public Map<OccurrenceIssue, LongAdder> process(DataFile dataFile) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private void aa(DataFile dataFile) {
    try (BufferedReader br = new BufferedReader(new FileReader(dataFile.getFileName()))) {
      String line;
      if(dataFile.isHasHeaders()) {
        br.readLine();
      }
      while ((line = br.readLine()) != null) {
        collector.accumulate(recordProcessor.process(line));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
