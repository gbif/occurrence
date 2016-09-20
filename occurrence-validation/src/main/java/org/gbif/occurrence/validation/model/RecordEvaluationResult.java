package org.gbif.occurrence.validation.model;

/**
 * WORK-IN-PROGRESS
 *
 * Class representing the result of an evaluation at the record level.
 */
public abstract class RecordEvaluationResult {

  public enum EvaluationType {STRUCTURE_EVALUATION, INTERPRETATION_BASED_EVALUATION}
  //public enum EvaluationResult {SKIPPED, PASSED, WARNING, FAILED} //not sure we need result, maybe level ?

  private final String recordId;
  private final EvaluationType evaluationType;

  public RecordEvaluationResult(String recordId, EvaluationType evaluationType){
    this.recordId = recordId;
    this.evaluationType = evaluationType;
  }

  public String getRecordId() {
    return recordId;
  }

  public EvaluationType getEvaluationType() {
    return evaluationType;
  }
}
