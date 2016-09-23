package org.gbif.occurrence.validation.api;

import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.Map;

public class DataFileValidationResult {

  public Map<OccurrenceIssue, Long> issues;

  public DataFileValidationResult(Map<OccurrenceIssue, Long> issues) {
    this.issues = issues;
  }

  public Map<OccurrenceIssue, Long> getIssues() {
    return issues;
  }

  public void setIssues(Map<OccurrenceIssue, Long> issues) {
    this.issues = issues;
  }

  @Override
  public String toString() {
    return "DataFileValidationResult{" +
           "issues=" + issues +
           '}';
  }
}
