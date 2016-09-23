package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;
import org.gbif.occurrence.validation.util.TempTermsUtils;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.Callable;

import akka.actor.UntypedActor;
import static akka.dispatch.Futures.future;

import static akka.pattern.Patterns.pipe;

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
    pipe(future(new Callable<DataWorkResult>() {
      @Override
      public DataWorkResult call() throws Exception {
        try( RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()),
                                                                           dataFile.getDelimiterChar(), dataFile.isHasHeaders(),
                                                                           TempTermsUtils.buildTermMapping(dataFile.getColumns()))){

          RecordInterpretionBasedEvaluationResult result;

          Map<Term, String> record;
          while ((record = recordSource.read()) != null) {
            result = recordProcessor.process(record);
            getSender().tell(result);
          }


          //add reader aggregated result to the DataWorkResult
          return new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS);
        } catch (Exception ex) {
          return new DataWorkResult(dataFile, DataWorkResult.Result.FAILED);
        }
      }
    }, getContext().dispatcher())).to(getSender());
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
