package org.gbif.occurrence.validation.tabular.single;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.HashMap;
import java.util.Map;

public class SimpleValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Long>> {

  private HashMap<OccurrenceIssue, Long> issuesCounter = new HashMap(OccurrenceIssue.values().length);
  private long recordCount;

  @Override
  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount += 1;
    result.getDetails().forEach(
      detail -> issuesCounter.compute(detail.getIssueFlag(), (k,v) -> (v == null) ? 1 : ++v)
    );
  }

  @Override
  public Map<OccurrenceIssue, Long> getAggregatedResult(){
    return issuesCounter;
  }

  @Override
  public String toString() {
    return "Record count: " + recordCount + " Issues: " + issuesCounter.toString();
  }

}
