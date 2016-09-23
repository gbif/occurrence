package org.gbif.occurrence.validation.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

/**
 *
 * WORK-IN-PROGRESS
 *
 * Result of an Interpretation Based Evaluation
 */
public class RecordInterpretionBasedEvaluationResult extends RecordEvaluationResult {

  private final List<Details> details;

  public RecordInterpretionBasedEvaluationResult(String recordId,
                                                 List<Details> details){
    super(recordId, EvaluationType.INTERPRETATION_BASED_EVALUATION);
    this.details = details;
  }

  public List<Details> getDetails() {
    return details;
  }

  public static class Builder {
    public List<Details> details;

    public Builder addDetail(OccurrenceIssue issueFlag, Map<Term, String> relatedData){
      if(details == null){
        details = Lists.newArrayList();
      }
      details.add(new Details(issueFlag, relatedData));
      return this;
    }

    public RecordInterpretionBasedEvaluationResult build(){
      return new RecordInterpretionBasedEvaluationResult("", details);
    }
  }

  /**
   * Contains details of a RecordInterpretionBasedEvaluationResult.
   */
  public static class Details {
    private final OccurrenceIssue issueFlag;
    private final Map<Term, String> relatedData;

    public Details(OccurrenceIssue issueFlag, Map<Term, String> relatedData){
      this.issueFlag = issueFlag;
      this.relatedData = relatedData;
    }

    public OccurrenceIssue getIssueFlag() {
      return issueFlag;
    }

    public Map<Term, String> getRelatedData() {
      return relatedData;
    }
  }
}
