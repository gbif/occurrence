package org.gbif.occurrence.validation.tabular.processor;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;


public class OccurrenceLineProcessor implements RecordProcessor {

  private final OccurrenceInterpreter interpreter;

  public OccurrenceLineProcessor(OccurrenceInterpreter interpreter) {
    this.interpreter = interpreter;
  }

  @Override
  public RecordInterpretionBasedEvaluationResult process(Map<Term, String> record) {
    //TODO maybe we should copy the fields?
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimFields(record);
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return toEvaluationResult("id", interpreter.interpret(verbatimOccurrence));
  }

  /**
   *
   * Creates a RecordInterpretionBasedEvaluationResult from an OccurrenceInterpretationResult.
   *
   * @param id
   * @param result
   *
   * @return
   */
  private RecordInterpretionBasedEvaluationResult toEvaluationResult(String id, OccurrenceInterpretationResult result) {

    //should we avoid creating an object (return null) or return an empty object?
    if(result.getUpdated().getIssues().isEmpty()){
      return null;
    }

    RecordInterpretionBasedEvaluationResult.Builder builder = new RecordInterpretionBasedEvaluationResult.Builder();
    Map<Term, String> verbatimFields = result.getOriginal().getVerbatimFields();
    Map<Term, String> relatedData;

    for (OccurrenceIssue issue : result.getUpdated().getIssues()) {
      if (InterpretationRemarksDefinition.REMARKS_MAP.containsKey(issue)) {
        relatedData = InterpretationRemarksDefinition.getRelatedTerms(issue)
          .stream()
          .filter(t -> verbatimFields.get(t) != null)
          .collect(Collectors.toMap(Function.identity(), t -> verbatimFields.get(t)));
        builder.addDetail(issue, relatedData);
      }
    }
    return builder.build();
  }
}
