package org.gbif.occurrence.validation.model;

/**
 * WORK-IN-PROGRESS
 */
public class RecordStructureEvaluationResult extends RecordEvaluationResult {

  private final String details;

  public RecordStructureEvaluationResult(String recordId, String details){
    super(recordId, EvaluationType.STRUCTURE_EVALUATION);
    this.details = details;
  }

  public String getDetails() {
    return details;
  }
}
