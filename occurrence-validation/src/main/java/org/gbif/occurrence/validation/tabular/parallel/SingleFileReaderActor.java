package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;

import akka.actor.UntypedActor;

public class SingleFileReaderActor extends UntypedActor {

  private final RecordProcessor recordProcessor;

  public SingleFileReaderActor(RecordProcessor recordProcessor) {
    this.recordProcessor = recordProcessor;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof RecordSource) {
      doWork((RecordSource) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(RecordSource recordSource) throws IOException {
    try {
      RecordInterpretionBasedEvaluationResult result;

        Map<Term, String> record;
        while ((record = recordSource.read()) != null) {
          result = recordProcessor.process(record);
          getSender().tell(result);
        }

      //add reader aggregated result to the DataWorkResult
      //FIXME new DataFile()
      getSender().tell(new DataWorkResult(new DataFile(), DataWorkResult.Result.SUCCESS));
    } catch (Exception ex) {
      getSender().tell(new DataWorkResult(new DataFile(), DataWorkResult.Result.FAILED));
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
