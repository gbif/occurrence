package org.gbif.occurrence.validation.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;

/**
 *
 * WORK-IN-PROGRESS
 *
 * Result of an Interpretation Based Evaluation
 */
public class RecordInterpretionBasedEvaluationResult extends RecordEvaluationResult {

  private final List<RecordValidationResultDetails> details;

  public RecordInterpretionBasedEvaluationResult(String recordId,
                                                 List<RecordValidationResultDetails> details){
    super(recordId, EvaluationType.INTERPRETATION_BASED_EVALUATION);
    this.details = details;
  }

  /**
   *
   */
  public static class RecordValidationResultDetails {
    private final OccurrenceIssue issueFlag;
    private final Map<Term, String> relatedData;

    public RecordValidationResultDetails(OccurrenceIssue issueFlag, Map<Term, String> relatedData){
      this.issueFlag = issueFlag;
      this.relatedData = relatedData;
    }

  }
}
