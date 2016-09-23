package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.tabular.single.SingleDataFileProcessor;

import java.io.IOException;
import java.text.MessageFormat;

import akka.actor.UntypedActor;

public class SingleFileReaderActor extends UntypedActor {

  private final RecordProcessor recordProcessor;

  public SingleFileReaderActor(RecordProcessor recordProcessor) {
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

    try (SingleDataFileProcessor.DataFileReader reader = new SingleDataFileProcessor.DataFileReader(dataFile,recordProcessor)) {
      RecordInterpretionBasedEvaluationResult result;
      while ((result = reader.read()) != null) {
        getSender().tell(result);
      }
      //add reader aggregated result to the DataWorkResult
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
