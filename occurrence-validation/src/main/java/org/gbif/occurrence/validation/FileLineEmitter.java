package org.gbif.occurrence.validation;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.model.RecordStructureEvaluationResult;
import org.gbif.tabular.MappedTabularDataFileReader;
import org.gbif.tabular.MappedTabularDataLine;
import org.gbif.tabular.MappedTabularFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import akka.actor.UntypedActor;

public class FileLineEmitter extends UntypedActor {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();
  private OccurrenceInterpreter interpreter;

  public FileLineEmitter(OccurrenceInterpreter interpreter) {
    this.interpreter = interpreter;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataInputFile) {
      doWork((DataInputFile) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(DataInputFile dataInputFile) throws IOException {

    //Build the columns mapping from Strings
    String[] columnsName = dataInputFile.getColumns();
    Term[] columnsMapping = new Term[columnsName.length];
    for (int i = 0; i < columnsName.length; i++) {
      columnsMapping[i] = TERM_FACTORY.findTerm(columnsName[i]);
    }

    try (MappedTabularDataFileReader<Term> mappedTabularFileReader =
                 MappedTabularFiles.newTermMappedTabularFileReader(new FileInputStream(
                                 new File(dataInputFile.getFileName())), dataInputFile.getDelimiterChar(),
                         dataInputFile.isHasHeaders(), columnsMapping)) {
      MappedTabularDataLine<Term> line;
      while ((line = mappedTabularFileReader.read()) != null) {

        if(line.getNumberOfColumn() != columnsMapping.length){
          //getSender().tell(toColumnCountMismatchEvaluationResult(line.getLineNumber(), columnsMapping.length,
          //        line.getNumberOfColumn()));
        }

        OccurrenceInterpretationResult result = interpreter.interpret(toVerbatimOccurrence(line.getMappedData()));
        getSender().tell(result);
        //getSender().tell(toEvaluationResult(result));
      }
      getSender().tell(new DataWorkResult(dataInputFile, DataWorkResult.Result.SUCCESS));
    } catch (Exception ex) {
      getSender().tell(new DataWorkResult(dataInputFile, DataWorkResult.Result.FAILED));
    }
  }

  /**
   * Get a {@link VerbatimOccurrence} instance from a {@Map} of {@link Term}
   *
   * @param line
   *
   * @return
   */
  private VerbatimOccurrence toVerbatimOccurrence(Map<Term, String> line) {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimFields(line);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return verbatimOccurrence;
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
      relatedData = InterpretationRemarksDefinition.REMARKS.stream()
              //FIXME rewrite in a more efficient way: InterpretationRemarksDefinition should also have the mapping with the
              //OccurrenceIssue as key
              .filter(r -> issue.equals(r.getType()))
              .map(r -> r.getRelatedTerms())
              .flatMap(Collection::stream)
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
