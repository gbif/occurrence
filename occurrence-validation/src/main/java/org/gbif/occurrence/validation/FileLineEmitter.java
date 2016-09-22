package org.gbif.occurrence.validation;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;

import akka.actor.UntypedActor;

public class FileLineEmitter<T> extends UntypedActor {

  private final RecordProcessor<T> recordProcessor;

  public FileLineEmitter(RecordProcessor<T> recordProcessor) {
    this.recordProcessor = recordProcessor;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataFile) {
      doWork((DataFile) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(DataFile dataFile) throws IOException {

    try (BufferedReader br = new BufferedReader(new FileReader(dataFile.getFileName()))) {
      String line;
      if(dataFile.isHasHeaders()) {
        br.readLine();
      }
      while ((line = br.readLine()) != null) {
        getSender().tell(recordProcessor.process(line));
      }
      getSender().tell(new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS));
    } catch (Exception ex) {
      getSender().tell(new DataWorkResult(dataFile, DataWorkResult.Result.FAILED));

    }
  }




  /**
   * WORK-IN-PROGRESS
   *
   * @param lineNumber
   * @param expectedColumnCount
   * @param actualColumnCount
   * @return
   */
  private static RecordStructureEvaluationResult toColumnCountMismatchEvaluationResult(int lineNumber, int expectedColumnCount,
                                                                            int actualColumnCount) {
    return new RecordStructureEvaluationResult(Integer.toString(lineNumber),
            MessageFormat.format("Column count mismatch: expected {0} columns, got {1} columns",
            expectedColumnCount, actualColumnCount));
  }


}
