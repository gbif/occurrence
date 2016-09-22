package org.gbif.occurrence.validation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import akka.actor.UntypedActor;

public class FileLineEmitter<T> extends UntypedActor {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

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
        line = br.readLine();
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
   * Creates a RecordInterpretionBasedEvaluationResult from an OccurrenceInterpretationResult.
   *
   * @param id
   * @param result
   *
   * @return
   */
  private RecordInterpretionBasedEvaluationResult toEvaluationResult(String id, OccurrenceInterpretationResult result) {
    //FIXME reduce verboseness
    List<RecordInterpretionBasedEvaluationResult.RecordValidationResultDetails> details = new ArrayList();
    Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();
    Map<Term, String> relatedData;

    for (OccurrenceIssue issue : result.getUpdated().getIssues()) {
      relatedData = InterpretationRemarksDefinition.getRelatedTerms(issue).stream()
              .filter(t -> verbatimFields.get(t) != null)
              .collect(Collectors.toMap(Function.identity(),
                      t -> verbatimFields.get(t)));
      details.add(
              new RecordInterpretionBasedEvaluationResult.RecordValidationResultDetails(issue, relatedData));
    }
    return new RecordInterpretionBasedEvaluationResult(id, details);
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
