package org.gbif.occurrence.validation.tabular;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class SingleDataFileProcessor implements DataFileProcessor<Map<OccurrenceIssue, Long>> {

  public static class DataFileReader  implements Closeable {

    private BufferedReader reader;

    private final RecordProcessor recordProcessor;

    public DataFileReader(DataFile dataFile, RecordProcessor recordProcessor) {
      this.recordProcessor = recordProcessor;
      try {
        reader = new BufferedReader(new FileReader(dataFile.getFileName()));
        if(dataFile.isHasHeaders()) {
          reader.readLine();
        }
      } catch (Exception ex){
         throw new IllegalArgumentException(ex);
      }
    }

    public RecordInterpretionBasedEvaluationResult read() throws IOException {
      String line = reader.readLine();
      if (line != null) {
        return recordProcessor.process(line);
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }
  }
  private final RecordProcessor recordProcessor;

  private final SimpleValidationCollector collector;

  public SingleDataFileProcessor(RecordProcessor recordProcessor) {
    this.recordProcessor = recordProcessor;
    collector = new SimpleValidationCollector();
  }

  @Override
  public Map<OccurrenceIssue, Long> process(DataFile dataFile) {
    try (DataFileReader dataFileReader = new DataFileReader(dataFile, recordProcessor)) {
      RecordInterpretionBasedEvaluationResult result;
      while ((result = dataFileReader.read()) != null) {
        collector.accumulate(result);
      }
      return collector.getAggregatedResult();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


}
